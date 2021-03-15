package spark.jobserver

import akka.actor._
import akka.cluster.Cluster
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.testkit._
import akka.cluster.ClusterEvent.MemberEvent
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io._
import ContextSupervisor._
import spark.jobserver.util.{ContextJVMInitializationTimeout, ManagerLauncher,
  SparkJobUtils, Utils, JobserverConfig}
import spark.jobserver.JobManagerActor._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.reflect.ClassTag
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import akka.util.Timeout
import spark.jobserver.io.JobDAOActor._

import java.time.ZonedDateTime

object AkkaClusterSupervisorActorSpec {
  // All the Actors System should have the same name otherwise they cannot form a cluster
  val ACTOR_SYSTEM_NAME = "test"

  val config = ConfigFactory.parseString("""
    akka {
      # Disable all akka output to console
      log-dead-letters = 0
      loglevel = "OFF" # Other options INFO, OFF, DEBUG, WARNING
      stdout-loglevel = "OFF"
      log-dead-letters-during-shutdown = off
      cluster.log-info = off
      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
        warn-about-java-serializer-usage = off
      }
      remote.netty.tcp.hostname = "127.0.0.1"
      cluster.auto-down-unreachable-after = 3s
    }
    spark {
      master = "local[4]"
      driver.supervise = false
      temp-contexts {
        num-cpu-cores = 1           # Number of cores to allocate.  Required.
        memory-per-node = 256m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
      }
      jobserver.job-result-cache-size = 100
      jobserver.context-creation-timeout = 5 s
      jobserver.dao-timeout = 3 s
      jobserver.context-deletion-timeout= 3 s
      context-per-jvm = true
      contexts {
        config-context {
          num-cpu-cores = 4
          memory-per-node = 512m
        }
      }
      context-settings {
        num-cpu-cores = 1
        memory-per-node = 256m
        context-init-timeout = 2 s
        forked-jvm-init-timeout = 10s
        context-factory = spark.jobserver.context.DefaultSparkContextFactory
        passthrough {
          spark.driver.allowMultipleContexts = true
          spark.ui.enabled = false
        }
      }
    }
    """)

  val system = ActorSystem(ACTOR_SYSTEM_NAME, config)
}

object StubbedAkkaClusterSupervisorActor {
  case class AddContextToContextInitInfos(contextName: String)
  case object DisableDAOCommunication
  case object EnableDAOCommunication
  case class DummyTerminated(actorRef: ActorRef)
}

class StubbedAkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef,
                                        managerProbe: TestProbe, cluster: Cluster,
                                        visitedContextIsFinalStatePath: AtomicInteger)
        extends AkkaClusterSupervisorActor(daoActor, dataManagerActor, cluster) {

  override def isContextInFinalState(contextInfo: ContextInfo): Boolean = {
    if (super.isContextInFinalState(contextInfo)) {
      visitedContextIsFinalStatePath.incrementAndGet()
      return true;
    } else {
      return false;
    }
  }

  override def preStart(): Unit = {
    cluster.join(selfAddress)
    cluster.subscribe(self, classOf[MemberEvent])
  }

  var daoCommunicationDisabled = false
  def createSlaveClusterWithJobManager(contextName: String, contextConfig: Config): (Cluster, ActorRef) = {
    val managerConfig = ConfigFactory.
      parseString("akka.cluster.roles=[manager],akka.remote.netty.tcp.port=0").withFallback(config)
    val managerSystem = ActorSystem(AkkaClusterSupervisorActorSpec.ACTOR_SYSTEM_NAME, managerConfig)

    val stubbedJobManagerRef = managerSystem.actorOf(
      Props(classOf[StubbedJobManagerActor], contextConfig), contextName
    )
    val cluster = Cluster(managerSystem)
    managerProbe.watch(stubbedJobManagerRef)
    (cluster, stubbedJobManagerRef)
  }

  override protected def launchDriver(name: String, contextConfig: Config,
                                      contextActorName: String): (Boolean, String) = {
    // Create probe and cluster and join back the master
    val shouldDriverLaunchFail = Try(contextConfig.getBoolean("driver.fail")).getOrElse(false)
    val shouldDriverFailToJoinCluster = Try(contextConfig.getBoolean(
      "driver.cluster.join.fail")).getOrElse(false)
    (shouldDriverLaunchFail, shouldDriverFailToJoinCluster) match {
      case (true, false) | (true, true) => (false, "")
      case (false, true) => (true, "")
      case (false, false) =>
        val managerActorAndCluster = createSlaveClusterWithJobManager(contextActorName, contextConfig)
        managerActorAndCluster._1.join(selfAddress)
        (true, "")
    }
  }

  override def wrappedReceive: Receive = {
    stubbedWrappedReceive.orElse(super.wrappedReceive)
  }

  def stubbedWrappedReceive: Receive = {
    case StubbedAkkaClusterSupervisorActor.AddContextToContextInitInfos(name) =>
      contextInitInfos(name) = ({ref => }, {ref => }, new Cancellable {
        def cancel(): Boolean = { false }
        def isCancelled: Boolean = { false }
      })
    case StubbedAkkaClusterSupervisorActor.DisableDAOCommunication =>
      daoCommunicationDisabled = true
    case StubbedAkkaClusterSupervisorActor.EnableDAOCommunication =>
      daoCommunicationDisabled = false
    case StubbedAkkaClusterSupervisorActor.DummyTerminated(actorRef) =>
      handleTerminatedEvent(actorRef)
      sender ! "Executed"
    case Terminated(actorRef) =>
      handleTerminatedEvent(actorRef)
      managerProbe.ref ! "Executed"
  }

  override def getDataFromDAO[T: ClassTag](msg: JobDAOActor.JobDAORequest): Option[T] = {
    daoCommunicationDisabled match {
      case true => None
      case false => super.getDataFromDAO[T](msg)
    }
  }

  override def leaveCluster(actorRef: ActorRef): Unit = {
    actorRef.path.address.toString match {
      case "akka://test" =>
      // If we use TestProbe and leave the cluster then the master itself leaves the cluster.
      // For such cases, we don't do anything. For other normal cases, we leave the cluster.
      case _ => context.stop(actorRef)
    }
  }
}

class StubbedJobManagerActor(contextConfig: Config) extends Actor {
  var contextName: String = _
  var stopAttemptCount: Int = 0
  def receive: Receive = {
    case JobManagerActor.Initialize(contextConfig, _, _) =>
      contextName = contextConfig.getString("context.name")
      val resultActor = context.system.actorOf(Props(classOf[JobResultActor]))
      sender() ! JobManagerActor.Initialized(contextConfig.getString("context.name"), resultActor)
    case JobManagerActor.GetContexData =>
      val appId = Try(contextConfig.getString("manager.context.appId")).getOrElse("")
      val webUiUrl = Try(contextConfig.getString("manager.context.webUiUrl")).getOrElse("")
      (appId, webUiUrl) match {
        case ("Error", _) => sender() ! new Throwable("Some Exception")
        case ("", "") => sender() ! JobManagerActor.SparkContextDead
        case (_, "") => sender() ! JobManagerActor.ContexData(appId, None)
        case (_, _) => sender() ! JobManagerActor.ContexData(appId, Some(webUiUrl))
      }
    case JobManagerActor.StopContextAndShutdown =>
      contextName match {
        case "send-in-progress-back" =>
          stopAttemptCount match {
            case 0 =>
              stopAttemptCount += 1
              sender ! ContextStopInProgress
            case 1 =>
              sender ! SparkContextStopped
              self ! PoisonPill
          }

        case "multiple-stop-attempts" => stopAttemptCount match {
          case 0 | 1 =>
            stopAttemptCount += 1
            sender ! ContextStopInProgress
          case 2 =>
            sender ! SparkContextStopped
            self ! PoisonPill
          }
        case "dont-respond" =>
          stopAttemptCount match {
            case 0 => stopAttemptCount += 1
            case 1 =>
              sender ! SparkContextStopped
              self ! PoisonPill
          }
        case _ =>
          sender ! SparkContextStopped
          self ! PoisonPill
      }
    case JobManagerActor.StopContextForcefully =>
      contextName match {
        case "send-on-error" =>
          sender ! ContextStopError(new Exception("Some exception"))
        case "dont-respond-if-force" =>
        case _ =>
          sender ! SparkContextStopped
          self ! PoisonPill
      }
    case unexpectedMsg @ _ =>
      println(s"Unexpected message received: $unexpectedMsg")
  }
}

class AkkaClusterSupervisorActorSpec extends TestKit(AkkaClusterSupervisorActorSpec.system)
  with ImplicitSender with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  implicit val futureTimeout = Timeout(5.seconds)
  private val daoTimeout = 5.seconds.dilated
  private val contextInitTimeout = 10.seconds.dilated
  private var supervisor: ActorRef = _
  private var inMemoryMetaDAO: MetaDataDAO = _
  private var inMemoryBinDAO: BinaryDAO = _
  private var daoActor: ActorRef = _
  private var managerProbe = TestProbe()
  private val contextConfig = AkkaClusterSupervisorActorSpec.config.getConfig("spark.context-settings")
  private val unusedDummyInput = 1
  private val visitedContextIsFinalStatePath = new AtomicInteger(0)
  private lazy val daoConfig: Config = ConfigFactory.load("local.test.dao.conf")
  // This is needed to help tests pass on some MBPs when working from home
  System.setProperty("spark.driver.host", "localhost")

  def saveContextAndJobInRestartingState(contextId: String) : String = {
    val dt = ZonedDateTime.now()
    saveContextInSomeState(contextId, ContextStatus.Restarting)
    val job = JobInfo("specialJobId", contextId, "someContext",
        "com.abc.meme", JobStatus.Restarting, dt, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt)))
    Await.result(daoActor ? SaveJobInfo(job), daoTimeout)
    job.jobId
  }

  def saveContextInSomeState(contextId: String, state: String) : ContextInfo = {
    val dt = ZonedDateTime.now()
    val configWithSuperviseMode = ConfigFactory.parseString(
        s"${ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY}=true, is-adhoc=false, context.name=someContext," +
          s"context.id=$contextId")
        .withFallback(contextConfig)
    val convertedContextConfig = configWithSuperviseMode.root().render(ConfigRenderOptions.concise())
    val context = ContextInfo(contextId, "someContext", convertedContextConfig, None, dt, None, state, None)
    saveContextInfo(context)
    (context)
  }

  def setContextState(contextName: String, state: String): ContextInfo = {
    daoActor ! JobDAOActor.GetContextInfoByName(contextName)
    val msg = expectMsgType[JobDAOActor.ContextResponse]
    val currentContext = msg.contextInfo.get
    setContextState(currentContext, state)
  }

  def setContextState(contextInfo: ContextInfo, state: String): ContextInfo = {
    val updatedContext = contextInfo.copy(state = state,
      endTime = Some(ZonedDateTime.now()))
    saveContextInfo(updatedContext)
    updatedContext
  }

  def saveContextInfo(contextInfo: ContextInfo): Unit = {
    daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
    expectMsg(JobDAOActor.SavedSuccessfully)
  }

  override def beforeAll() {
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryDAO
    daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
    val cluster = Cluster(system)
    supervisor = system.actorOf(Props(classOf[StubbedAkkaClusterSupervisorActor], daoActor, TestProbe().ref,
        managerProbe, cluster, visitedContextIsFinalStatePath), "supervisor")
  }

  override def afterAll(): Unit = {
     AkkaTestUtils.shutdownAndWait(AkkaClusterSupervisorActorSpec.system)
  }

  after {
    // Cleanup all the context to have a fresh start for next testcase
    def stopContext(contextName: Any) {
      supervisor ! StopContext(contextName.toString())
      expectMsg(3.seconds.dilated, ContextStopped)
      managerProbe.expectMsgClass(classOf[Terminated])
      managerProbe.expectMsg("Executed")
    }

    supervisor ! ListContexts
    expectMsgPF(3.seconds.dilated) {
      case contexts: Seq[_] => contexts.foreach(stopContext(_))
      case _ =>
    }
  }

  describe("Context create tests") {
    it("should be able to start a context") {
      supervisor ! AddContext("test-context", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)
    }

    it("should return valid managerActorRef if context exists") {
      supervisor ! AddContext("test-context1", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! GetContext("test-context1")
      val isValid = expectMsgPF(2.seconds.dilated) {
        case _: ActorRef => true
        case _ => false
      }

      isValid should be (true)
    }

    it("should not create context in case of error") {
      val wrongConfig = ConfigFactory.parseString("driver.fail=true").withFallback(contextConfig)
      supervisor ! AddContext("test-context2", wrongConfig)
      expectMsgClass(classOf[ContextInitError])

      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }

    it("should not start another context with same name if first one is in non final state") {
      val contextName = "context-non-final"
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      // Running
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextAlreadyExists)

      val startedContext = setContextState(contextName, ContextStatus.Started)
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextAlreadyExists)

      setContextState(startedContext, ContextStatus.Restarting)
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextAlreadyExists)

      setContextState(startedContext, ContextStatus.Stopping)
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextAlreadyExists)
    }

    it("should be able to add contexts from config") {
      supervisor ! AddContextsFromConfig
      Thread.sleep(contextInitTimeout.toMillis) // AddContextsFromConfig does not return any message

      supervisor ! ListContexts
      expectMsg(Seq("config-context"))
    }

    it("should be able to start adhoc context and list it") {
      import spark.jobserver.util.SparkJobUtils
      supervisor ! StartAdHocContext("test-adhoc-classpath", ConfigFactory.parseString(""))

      val isValid = expectMsgPF(contextInitTimeout, "manager and result actors") {
        case manager: ActorRef =>
          manager.path.name.startsWith("jobManager-")
      }

      isValid should be (true)

      supervisor ! ListContexts
      val hasContext = expectMsgPF(3.seconds.dilated) {
        case contexts: Seq[_] =>
          contexts.head.toString.endsWith("test-adhoc-classpath")
        case _ => false
      }
      hasContext should be (true)
    }

    it("should be able to stop a running context") {
      supervisor ! AddContext("test-context4", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("test-context4")
      expectMsg(ContextStopped)
      managerProbe.expectMsgClass(classOf[Terminated])
      managerProbe.expectMsg("Executed")
    }

    it("context stop should be able to handle case when no context is present") {
      supervisor ! StopContext("test-context5")
      expectMsg(NoSuchContext)
    }

    it("should respond with ContextStopInProgress if driver failed to stop") {
      supervisor ! AddContext("send-in-progress-back", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("send-in-progress-back")

      expectMsg(ContextStopInProgress)
    }

    it("should respond with context stop error if driver doesn't respond back") {
      supervisor ! AddContext("dont-respond", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("dont-respond")

      expectMsgType[ContextStopError](4.seconds)
    }

    it("should stop context eventually if context stop was in progress") {
      supervisor ! AddContext("multiple-stop-attempts", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("multiple-stop-attempts")
      expectMsg(ContextStopInProgress)

      supervisor ! StopContext("multiple-stop-attempts")
      expectMsg(ContextStopInProgress)

      supervisor ! StopContext("multiple-stop-attempts")
      expectMsg(ContextStopped)
      managerProbe.expectMsgClass(classOf[Terminated])
      managerProbe.expectMsg("Executed")
    }

    it("should be able to stop a running context forcefully") {
      supervisor ! AddContext("test-context6", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("test-context6", true)
      expectMsg(ContextStopped)
      managerProbe.expectMsgClass(classOf[Terminated])
      managerProbe.expectMsg("Executed")
    }

    it("should respond with context stop error if there is no answer after forcefull stop request") {
      supervisor ! AddContext("dont-respond-if-force", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("dont-respond-if-force", true)

      expectMsgType[ContextStopError](4.seconds)
    }

    it("should respond with context stop error if error was sent back") {
      supervisor ! AddContext("send-on-error", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("send-on-error", true)

      expectMsgType[ContextStopError](4.seconds)
    }

    it("should set context and nonfinal jobs to error state if actor resolution fails during StopContext"){
      val contextWithoutActor = ContextInfo("contextWithoutActor", "contextWithoutActor", "", None,
          ZonedDateTime.now(), None, ContextStatus.Running, None)
      val finalJob = JobInfo("finalJob", "contextWithoutActor", "contextWithoutActor",
          "", JobStatus.Finished, ZonedDateTime.now(),
          Some(ZonedDateTime.now()), None, Seq(BinaryInfo("demo", BinaryType.Jar, ZonedDateTime.now())))
      val nonfinalJob = JobInfo("nonfinalJob", "contextWithoutActor", "contextWithoutActor",
          "", JobStatus.Running, ZonedDateTime.now(),
          None, None, Seq(BinaryInfo("demo", BinaryType.Jar, ZonedDateTime.now())))
      Await.result(daoActor ? SaveContextInfo(contextWithoutActor), daoTimeout)
      Await.result(daoActor ? SaveJobInfo(finalJob), daoTimeout)
      Await.result(daoActor ? SaveJobInfo(nonfinalJob), daoTimeout)

      supervisor ! StopContext("contextWithoutActor")

      expectMsg(4.seconds, NoSuchContext)
      Utils.retry(3, 1000){
        // If one of these assertion fails, it's probably because the overall flow is faster than
        // the DAO update (sent with tell). In this case, just retry
        val f = Await.result(daoActor ? GetContextInfo(contextWithoutActor.id), 3. seconds).asInstanceOf[ContextResponse]
        val c = f.contextInfo.get
        c.state should equal(ContextStatus.Error)
        c.error.get.getClass.getSimpleName should equal("ResolutionFailedOnStopContextException")
        c.endTime.isDefined should equal(true)
        Await.result(daoActor ? GetJobInfo("finalJob"), 3. seconds) should equal(Some(finalJob))
        val j = Await.result(daoActor ? GetJobInfo("nonfinalJob"), 3. seconds).asInstanceOf[Option[JobInfo]].get
        j.state should equal(ContextStatus.Error)
        j.error.isDefined should equal(true)
        j.endTime.isDefined should equal(true)
      }
    }

    it("should be able to start multiple contexts") {
      supervisor ! AddContext("test-context7", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! AddContext("test-context8", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! ListContexts
      expectMsgAnyOf(Seq("test-context7", "test-context8"), Seq("test-context8", "test-context7"))
    }

    it("should kill context JVM if nothing was found in the DB and no callback was available") {
      val managerProbe = TestProbe("jobManager-dummy")
      val deathWatch = TestProbe()
      deathWatch.watch(managerProbe.ref)

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))

      deathWatch.expectTerminated(managerProbe.ref)
    }

    it("should kill context JVM if context was found in DB but no callback was available" +
        " and supervise mode is not enabled") {
      val managerProbe = TestProbe(JobserverConfig.MANAGER_ACTOR_PREFIX + "dummy")
      val contextId = managerProbe.ref.path.name.replace(JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val deathWatch = TestProbe()
      deathWatch.watch(managerProbe.ref)
      val dummyContext = ContextInfo(contextId, "contextName", "", None,
        ZonedDateTime.now(), None, ContextStatus.Started, None)
      Await.result(daoActor ? SaveContextInfo(dummyContext), daoTimeout)

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))

      deathWatch.expectTerminated(managerProbe.ref)
    }

    it("should kill context JVM and cleanup jobs if context was found in DB in STOPPING state") {
      val managerProbe = TestProbe(JobserverConfig.MANAGER_ACTOR_PREFIX + "stoppingContext")
      val contextId = managerProbe.ref.path.name.replace(JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val deathWatch = TestProbe()
      deathWatch.watch(managerProbe.ref)
      val dummyContext = ContextInfo(contextId, contextId, "", None,
        ZonedDateTime.now(), None, ContextStatus.Stopping, None)
      val finalJob = JobInfo("finalJob", contextId, contextId,
        "", JobStatus.Finished, ZonedDateTime.now(),
        Some(ZonedDateTime.now()), None, Seq(BinaryInfo("demo", BinaryType.Jar, ZonedDateTime.now())))
      val nonfinalJob = JobInfo("nonfinalJob", contextId, contextId,
        "", JobStatus.Running, ZonedDateTime.now(),
        None, None, Seq(BinaryInfo("demo", BinaryType.Jar, ZonedDateTime.now())))
      Await.result(daoActor ? SaveContextInfo(dummyContext), daoTimeout)
      Await.result(daoActor ? SaveJobInfo(finalJob), daoTimeout)
      Await.result(daoActor ? SaveJobInfo(nonfinalJob), daoTimeout)

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))

      deathWatch.expectTerminated(managerProbe.ref)

      Utils.retry(3, 1000){
        // If one of these assertion fails, it's probably because the overall flow is faster than
        // the DAO update (sent with tell). In this case, just retry
        val c = Await.result(daoActor ? GetContextInfo(contextId), 3. seconds).
          asInstanceOf[ContextResponse].contextInfo.get
        c.state should equal(ContextStatus.Error)
        c.endTime.isDefined should equal(true)
        Await.result(daoActor ? GetJobInfo("finalJob"), 3. seconds) should equal(Some(finalJob))
        val j = Await.result(daoActor ? GetJobInfo("nonfinalJob"), 3. seconds).asInstanceOf[Option[JobInfo]].get
        j.state should equal(ContextStatus.Error)
        j.error.isDefined should equal(true)
        j.endTime.isDefined should equal(true)
      }
    }

    it("should kill context JVM if DB call had an exception but callbacks are available") {
      val managerProbe = TestProbe("jobManager-dummy")
      val contextActorName = managerProbe.ref.path.name
      val deathWatch = TestProbe()
      deathWatch.watch(managerProbe.ref)

      supervisor ! StubbedAkkaClusterSupervisorActor.AddContextToContextInitInfos(contextActorName)
      supervisor ! StubbedAkkaClusterSupervisorActor.DisableDAOCommunication // Simulate DAO failure

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))
      deathWatch.expectTerminated(managerProbe.ref)
      expectNoMessage((SparkJobUtils.getForkedJVMInitTimeout(system.settings.config) + 1).second)

      supervisor ! StubbedAkkaClusterSupervisorActor.EnableDAOCommunication
    }

    it("should kill context JVM if DB call had an exception and callbacks are not available") {
      val managerProbe = TestProbe("jobManager-dummy")
      val deathWatch = TestProbe()
      deathWatch.watch(managerProbe.ref)
      supervisor ! StubbedAkkaClusterSupervisorActor.DisableDAOCommunication // Simulate DAO failure

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))
      deathWatch.expectTerminated(managerProbe.ref)

      supervisor ! StubbedAkkaClusterSupervisorActor.EnableDAOCommunication
    }

    /**
     * This is a common case in cluster-mode. If you submit a context to Spark cluster
     * and it does not have enough resources then context will be put to "SUBMITTED"
     * state by Spark. Since JVM was never initialized, no cluster was joined and user
     * received ERROR but later at some point when Spark cluster has resources, it will
     * move context to RUNNING state. At this point, this JVM will try to join the
     * Akka cluster but we don't need it anymore because user does not know about it.
     */
    it("should kill context if JVM creation timed out already and context is trying to join/initialize") {
      visitedContextIsFinalStatePath.set(0)
      val timedOutRejoiningManagerProbe = TestProbe("jobManager-timedOutManager")
      val deathWatch = TestProbe()
      deathWatch.watch(timedOutRejoiningManagerProbe.ref)

      val contextActorName = timedOutRejoiningManagerProbe.ref.path.name
      supervisor ! StubbedAkkaClusterSupervisorActor.AddContextToContextInitInfos(contextActorName)

      val timedOutContextId = timedOutRejoiningManagerProbe.ref.path.name.replace(JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val timedOutContext = ContextInfo(timedOutContextId, "contextName", "", None, ZonedDateTime.now(),
          Some(ZonedDateTime.now().plusHours(1)), ContextStatus.Error, Some(ContextJVMInitializationTimeout()))
      Await.result(daoActor ? SaveContextInfo(timedOutContext), daoTimeout)

      supervisor ! ActorIdentity(unusedDummyInput, Some(timedOutRejoiningManagerProbe.ref))

      deathWatch.expectTerminated(timedOutRejoiningManagerProbe.ref)
      visitedContextIsFinalStatePath.get() should be (1)
    }

    /**
     * If Jobserver restarts it will lose all entries in contextInitInfos
     */
    it("should kill context if JVM creation timed out already and context is trying to" +
      "join/initialize after Jobserver was restarted") {
      visitedContextIsFinalStatePath.set(0)
      val timedOutRejoiningManagerProbe = TestProbe(
        JobserverConfig.MANAGER_ACTOR_PREFIX + "timedOutManager")
      val finishRejoiningManagerProbe = TestProbe(
        JobserverConfig.MANAGER_ACTOR_PREFIX + "timedOutManager")

      val deathWatch = TestProbe()
      deathWatch.watch(timedOutRejoiningManagerProbe.ref)
      deathWatch.watch(finishRejoiningManagerProbe.ref)

      val timedOutContextId = timedOutRejoiningManagerProbe.ref.path.name.replace(
        JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val timedOutContext = ContextInfo(timedOutContextId, "timedOutContext", "", None, ZonedDateTime.now(),
          Some(ZonedDateTime.now().plusHours(1)), ContextStatus.Error, Some(ContextJVMInitializationTimeout()))
      Await.result(daoActor ? SaveContextInfo(timedOutContext), daoTimeout)

      val finishedContextId = finishRejoiningManagerProbe.ref.path.name.replace(
        JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val finishedContext = ContextInfo(finishedContextId, "finishedContext", "", None, ZonedDateTime.now(),
          Some(ZonedDateTime.now().plusHours(1)), ContextStatus.Finished, None)
      Await.result(daoActor ? SaveContextInfo(finishedContext), daoTimeout)

      supervisor ! ActorIdentity(unusedDummyInput, Some(timedOutRejoiningManagerProbe.ref))
      supervisor ! ActorIdentity(unusedDummyInput, Some(finishRejoiningManagerProbe.ref))

      deathWatch.expectTerminated(timedOutRejoiningManagerProbe.ref)
      deathWatch.expectTerminated(finishRejoiningManagerProbe.ref)
      visitedContextIsFinalStatePath.get() should be (2)
    }

    it("should kill context if it was marked as error in DAO and it is trying to join/initialize") {
      visitedContextIsFinalStatePath.set(0)
      val erroredOutRejoiningManagerProbe = TestProbe("jobManager-erroredOutManager")
      val deathWatch = TestProbe()
      deathWatch.watch(erroredOutRejoiningManagerProbe.ref)

      val contextActorName = erroredOutRejoiningManagerProbe.ref.path.name
      supervisor ! StubbedAkkaClusterSupervisorActor.AddContextToContextInitInfos(contextActorName)

      val erroredOutContextId = erroredOutRejoiningManagerProbe.ref.path.name.replace(
        JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val erroredOutContext = ContextInfo(erroredOutContextId, "contextName", "", None, ZonedDateTime.now(),
          Some(ZonedDateTime.now().plusHours(1)), ContextStatus.Error, Some(new Exception("random error")))
      Await.result(daoActor ? SaveContextInfo(erroredOutContext), daoTimeout)

      supervisor ! ActorIdentity(unusedDummyInput, Some(erroredOutRejoiningManagerProbe.ref))

      deathWatch.expectTerminated(erroredOutRejoiningManagerProbe.ref)
      visitedContextIsFinalStatePath.get() should be (1)
    }

    it("should not receive JVM creation timed out error if context was intialized properly") {
      supervisor ! AddContext("test-context-proper-init", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)
      expectNoMessage((SparkJobUtils.getForkedJVMInitTimeout(system.settings.config) + 1).second)
    }
  }

  describe("Other context operations tests") {
    it("should list empty context at startup") {
       supervisor ! ListContexts
       expectMsg(Seq.empty[String])
    }

    it("should return NoSuchContext if context is not available while processing GetContext") {
       supervisor ! GetContext("dummy-name")
       expectMsg(NoSuchContext)
    }

    it("should be able to list running/restarting/stopping contexts") {
      val contextName = "test-context9"
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      daoActor ! JobDAOActor.GetContextInfoByName(contextName)
      val msg = expectMsgType[JobDAOActor.ContextResponse]
      val runningContext = msg.contextInfo.get

      supervisor ! ListContexts
      expectMsg(Seq(contextName)) // Running context

      val stoppingContext = runningContext.copy(state = ContextStatus.Stopping,
        endTime = Some(ZonedDateTime.now()))
      daoActor ! JobDAOActor.SaveContextInfo(stoppingContext)
      expectMsg(JobDAOActor.SavedSuccessfully)

      supervisor ! ListContexts
      expectMsg(Seq(contextName)) // Stopping context

      val restartingContext =  stoppingContext.copy(state = ContextStatus.Restarting,
        endTime = Some(ZonedDateTime.now()))
      daoActor ! JobDAOActor.SaveContextInfo(restartingContext)
      expectMsg(JobDAOActor.SavedSuccessfully)

      supervisor ! ListContexts
      expectMsg(Seq(contextName)) // Restarting context
    }

    it("should return valid result actor") {
      supervisor ! AddContext("test-context10", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! GetResultActor("test-context10")
      expectMsgClass(classOf[ActorRef])
    }

    it("should return NoSuchContext if context is not available for GetSparkContextInfo") {
      supervisor ! GetSparkContexData("dummy-name")
      expectMsg(NoSuchContext)
    }

    it("should return valid appId and webUiUrl if context is running") {
      val configWithContextInfo = ConfigFactory.parseString(
        "manager.context.webUiUrl=dummy-url,manager.context.appId=appId-dummy"
      ).withFallback(contextConfig)
      supervisor ! AddContext("test-context11", configWithContextInfo)
      expectMsg(contextInitTimeout, ContextInitialized)

      Utils.retry(3, 1000) {
        val context = Await.result(daoActor ? GetContextInfoByName("test-context11"), 3 seconds).
          asInstanceOf[ContextResponse].contextInfo.get
        context.state should equal(ContextStatus.Running)

        supervisor ! GetSparkContexData("test-context11")
        expectMsg(SparkContexData(context,
          Some("appId-dummy"), Some("dummy-url")))
      }
    }

    it("should return valid contextInfo, appId but no webUiUrl") {
      val configWithContextInfo = ConfigFactory.parseString("manager.context.appId=appId-dummy")
                .withFallback(contextConfig)
      supervisor ! AddContext("test-context12", configWithContextInfo)
      expectMsg(contextInitTimeout, ContextInitialized)

      Utils.retry(3, 1000) {
        val context = Await.result(daoActor ? GetContextInfoByName("test-context12"), 3 seconds).
          asInstanceOf[ContextResponse].contextInfo.get
        context.state should equal(ContextStatus.Running)

        supervisor ! GetSparkContexData("test-context12")
        expectMsg(SparkContexData(context, Some("appId-dummy"), None))
      }
    }

    it("should return valid contextInfo and no appId or webUiUrl if SparkContextDead is received") {
      val configWithContextInfo = ConfigFactory.parseString("")
                .withFallback(contextConfig)
      supervisor ! AddContext("test-context13", configWithContextInfo)
      expectMsg(contextInitTimeout, ContextInitialized)

      Utils.retry(3, 1000) {
        val context = Await.result(daoActor ? GetContextInfoByName("test-context13"), 3 seconds).
          asInstanceOf[ContextResponse].contextInfo.get
        context.state should equal(ContextStatus.Running)

        supervisor ! GetSparkContexData("test-context13")
        expectMsg(SparkContexData(context, None, None))
      }
    }

    it("should return valid contextInfo and no appId or webUiUrl if Expception occurs") {
      val configWithContextInfo = ConfigFactory.parseString("manager.context.appId=Error")
                .withFallback(contextConfig)
      supervisor ! AddContext("test-context14", configWithContextInfo)
      expectMsg(contextInitTimeout, ContextInitialized)

      Utils.retry(3, 1000) {
        val context = Await.result(daoActor ? GetContextInfoByName("test-context14"), 3 seconds).
          asInstanceOf[ContextResponse].contextInfo.get
        context.state should equal(ContextStatus.Running)

        supervisor ! GetSparkContexData("test-context14")
        expectMsg(SparkContexData(context, None, None))
      }
    }

    it("should return UnexpectedError if a problem with db happens") {
      supervisor ! StubbedAkkaClusterSupervisorActor.DisableDAOCommunication // Simulate DAO failure
      supervisor ! GetSparkContexData("test-context15")
      expectMsg(UnexpectedError)
      supervisor ! StubbedAkkaClusterSupervisorActor.EnableDAOCommunication
    }

    it("should return NoSuchContext if the context does not exist") {
      supervisor ! GetSparkContexData("test-context-does-not-exist")
      // JobManagerActor Stub by default return NoSuchContext
      expectMsg(NoSuchContext)
    }

    it("should not allow to create the same context if in restarting state") {
      val contextId = "restartingContextId"
      val contextName = "restartingContextName"
      val convertedContextConfig = contextConfig.root().render(ConfigRenderOptions.concise())

      val contextInfoPF = ContextInfo(contextId, contextName, convertedContextConfig, None, ZonedDateTime.now(),
          None, _: String, None)
      val restartingContext = contextInfoPF(ContextStatus.Restarting)
      Await.result(daoActor ? SaveContextInfo(restartingContext), daoTimeout)

      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextAlreadyExists)

      Await.result(daoActor ? SaveContextInfo(contextInfoPF(ContextStatus.Finished)), daoTimeout) // cleanup
    }

    it("should not change final state to STOPPING state") {
      val daoProbe = TestProbe()
      val contextInfo = ContextInfo("id", "name", "", None, ZonedDateTime.now(), None, ContextStatus.Finished, None)
      var contextToTest: ContextInfo = contextInfo

      daoProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case JobDAOActor.GetContextInfoByName(_) =>
              sender ! JobDAOActor.ContextResponse(Some(contextInfo))
            case JobDAOActor.GetContextInfos(_, _) =>
              sender ! Seq.empty.map(JobDAOActor.ContextInfos)
              TestActor.KeepRunning
          }
          TestActor.KeepRunning
        }
      })

      val cluster = Cluster(system)
      val supervisor = system.actorOf(Props(classOf[StubbedAkkaClusterSupervisorActor], daoProbe.ref,
        TestProbe().ref, managerProbe, cluster, visitedContextIsFinalStatePath), "supervisor2")

      supervisor ! StopContext("name")

      contextToTest.state should be(ContextStatus.Finished)
      expectMsg(contextInitTimeout, NoSuchContext)
    }

    it("should change non final state to STOPPING state") {
      val daoProbe = TestProbe()
      val latch = new CountDownLatch(1)
      val contextInfo = ContextInfo("id", "name", "", None, ZonedDateTime.now(), None, ContextStatus.Running, None)
      var contextToTest: ContextInfo = contextInfo

      daoProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case JobDAOActor.GetContextInfoByName(_) =>
              sender ! JobDAOActor.ContextResponse(Some(contextInfo))
              TestActor.KeepRunning
            case JobDAOActor.SaveContextInfo(c) =>
              contextToTest = c
              latch.countDown()
              TestActor.NoAutoPilot
            case JobDAOActor.GetContextInfos(_, _) =>
              sender ! Seq.empty.map(JobDAOActor.ContextInfos)
              TestActor.KeepRunning
          }
        }
      })

      val cluster = Cluster(system)
      val supervisor = system.actorOf(Props(classOf[StubbedAkkaClusterSupervisorActor], daoProbe.ref,
        TestProbe().ref, managerProbe, cluster, visitedContextIsFinalStatePath), "supervisor3")

      supervisor ! StopContext("name")
      latch.await()

      contextToTest.state should be(ContextStatus.Stopping)
      expectMsg(contextInitTimeout, NoSuchContext)
    }

    it("should return JVM initialization timeout if context JVM doesn't join cluster within timout") {
      val failingContextName = "test-context-cluster-fail-join"
      val wrongConfig = ConfigFactory.parseString("driver.cluster.join.fail=true").withFallback(contextConfig)

      supervisor ! AddContext(failingContextName, wrongConfig)

      val timeoutExceptionMessage = ContextJVMInitializationTimeout().getMessage
      val msg = expectMsgClass((SparkJobUtils.getForkedJVMInitTimeout(system.settings.config) + 1).seconds,
          classOf[ContextInitError])
      msg.t.getMessage should be(timeoutExceptionMessage)

      val timedOutContext = Await.result(daoActor ? GetContextInfoByName(failingContextName), daoTimeout).
        asInstanceOf[ContextResponse].contextInfo
      timedOutContext.get.state should be(ContextStatus.Error)
      timedOutContext.get.error.get.getMessage should be(timeoutExceptionMessage)
    }

    it("should raise a Terminated event if an actor is killed and set the state accordingly") {

      val contextName = "ctxRunning"

      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      Utils.retry(3, 1000) {
        val runningContextInfo = Await.result(daoActor ? GetContextInfoByName(contextName), daoTimeout).
          asInstanceOf[ContextResponse].contextInfo.get
        runningContextInfo.state should equal(ContextStatus.Running)

        val runningContextActorRef = JobServer.getManagerActorRef(runningContextInfo, system).get
        runningContextActorRef should not be(None)

        val deathWatcher = TestProbe()
        deathWatcher.watch(runningContextActorRef)
        runningContextActorRef ! PoisonPill
        deathWatcher.expectTerminated(runningContextActorRef)

        Thread.sleep(3000)
        val updatedContext = Await.result(daoActor ? GetContextInfoByName(contextName), daoTimeout).
          asInstanceOf[ContextResponse].contextInfo
        updatedContext.get.state should be(ContextStatus.Killed)
      }
    }

    it("should not change final state to non-final within the Terminated event") {
      val contextId = "ctxFinalState"

      val finalStateContext = saveContextInSomeState(contextId, ContextStatus.getFinalStates().last)
      val managerProbe = system.actorOf(Props.empty, s"jobManager-$contextId")

      supervisor ! StubbedAkkaClusterSupervisorActor.DummyTerminated(managerProbe)

      expectMsg("Executed")
      daoActor ! JobDAOActor.GetContextInfo(contextId)
      val msg = expectMsgType[JobDAOActor.ContextResponse]
      msg.contextInfo.get.state should be(ContextStatus.getFinalStates().last)
      msg.contextInfo.get.endTime should be(finalStateContext.endTime)
    }
  }

  describe("Supervise mode tests") {
    it("should start context if supervise mode is disabled") {
      supervisor ! AddContext("test-context", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      val contextInfo = Await.result(daoActor ? GetContextInfoByName("test-context"), daoTimeout).
        asInstanceOf[ContextResponse].contextInfo
      contextInfo should not be None
    }

    it("should start adhoc context if supervise mode is disabled") {
      supervisor ! StartAdHocContext("test-adhoc-classpath", contextConfig)

      val isValid = expectMsgPF(contextInitTimeout, "manager and result actors") {
        case manager: ActorRef =>
          manager.path.name.startsWith("jobManager-")
      }

      isValid should be (true)
    }

    it("should start context with supervise mode enabled") {
      val configWithSuperviseMode = ConfigFactory.parseString(
          s"${ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY}=true").withFallback(contextConfig)
      supervisor ! AddContext("test-context", configWithSuperviseMode)
      expectMsg(contextInitTimeout, ContextInitialized)

      val contextInfo = Await.result(daoActor ? GetContextInfoByName("test-context"), daoTimeout).
        asInstanceOf[ContextResponse].contextInfo
      contextInfo should not be None
    }

    it("should set states of the jobs to ERROR if a context with ERROR state sends ActorIdentity") {
      val managerProbe = TestProbe(JobserverConfig.MANAGER_ACTOR_PREFIX + "dummy")
      managerProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case Initialize(_, _, _) => sender ! JobManagerActor.InitError(new Throwable)
          }
          TestActor.KeepRunning
        }
      })

      val contextId = managerProbe.ref.path.name.replace(JobserverConfig.MANAGER_ACTOR_PREFIX, "")

      val jobId: String = saveContextAndJobInRestartingState(contextId)
      val context = Await.result(daoActor ? GetContextInfo(contextId), daoTimeout).
        asInstanceOf[ContextResponse].contextInfo.get

      supervisor ! ActorIdentity(context, Some(managerProbe.ref))

      Thread.sleep(3000)
      val jobInfo = Await.result(daoActor ? GetJobInfo(jobId), daoTimeout).asInstanceOf[Option[JobInfo]].get
      jobInfo.state should be(JobStatus.Error)
    }

    it("should try to restart context if supervise mode is enabled") {
      val managerProbe = TestProbe(JobserverConfig.MANAGER_ACTOR_PREFIX + "123")
      managerProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case Initialize(_, _, _) => sender ! Initialized("", TestProbe().ref)
            case RestartExistingJobs =>
          }
          TestActor.KeepRunning
        }
      })

      val contextId = managerProbe.ref.path.name.replace(JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val context = saveContextInSomeState(contextId, ContextStatus.Started)

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))

      managerProbe.expectMsgClass(classOf[Initialize])
      managerProbe.expectMsg(RestartExistingJobs)

      // After restart the context status is RUNNING. The after{} block of this class,
      // lists all contexts and then tries to stop them. Since this manager slave is just a
      // TestProbe it's address doesn't get resolved so, it cannot be stopped. So, we change
      // the status to finish to cleanup.
      Await.result(daoActor ? SaveContextInfo(context.copy(state = ContextStatus.Finished)), daoTimeout)
    }

    it("should change states of a context and jobs inside to ERROR on an Error during restart") {
      val managerProbe = TestProbe(JobserverConfig.MANAGER_ACTOR_PREFIX + "1234")
      managerProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case Initialize(_, _, _) =>
          }
          TestActor.KeepRunning
        }
      })

      val contextId = managerProbe.ref.path.name.replace(JobserverConfig.MANAGER_ACTOR_PREFIX, "")
      val jobId = saveContextAndJobInRestartingState(contextId)
      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))

      Thread.sleep(3000)
      val context = Await.result(daoActor ? GetContextInfo(contextId), daoTimeout).
        asInstanceOf[ContextResponse].contextInfo
      context.get.state should be(ContextStatus.Error)
      val job = Await.result(daoActor ? GetJobInfo(jobId), daoTimeout).asInstanceOf[Option[JobInfo]]
      job.get.state should be(JobStatus.Error)
    }

    it("should set state restarting for context which was terminated" +
      "unexpectedly and had supervise mode enabled") {
      val contextId = "testid"

      val runningContext = saveContextInSomeState(contextId, ContextStatus.Running)
      val managerProbe = system.actorOf(Props.empty, s"jobManager-$contextId")
      val daoProbe = TestProbe()

      supervisor ! StubbedAkkaClusterSupervisorActor.DummyTerminated(managerProbe)

      expectMsg("Executed")
      daoActor ! JobDAOActor.GetContextInfo(contextId)
      val msg = expectMsgType[JobDAOActor.ContextResponse]
      msg.contextInfo.get.state should be(ContextStatus.Restarting)

      //cleanup
      daoActor ! JobDAOActor.SaveContextInfo(msg.contextInfo.get.copy(state = ContextStatus.Finished))
      expectMsg(JobDAOActor.SavedSuccessfully)
    }

    it("should set state killed for context which was terminated unexpectedly and supervise mode disabled") {
      val contextId = "testid2"
      val convertedContextConfig = contextConfig.root().render(ConfigRenderOptions.concise())

      val runningContext = ContextInfo(contextId, "c", convertedContextConfig, None, ZonedDateTime.now(),
        None, ContextStatus.Running, None)
      daoActor ! JobDAOActor.SaveContextInfo(runningContext)
      expectMsg(JobDAOActor.SavedSuccessfully)

      val managerProbe = system.actorOf(Props.empty, s"jobManager-$contextId")
      val daoProbe = TestProbe()

      supervisor ! StubbedAkkaClusterSupervisorActor.DummyTerminated(managerProbe)

      expectMsg("Executed")
      daoActor ! JobDAOActor.GetContextInfo(contextId)
      val msg = expectMsgType[JobDAOActor.ContextResponse]
      msg.contextInfo.get.state should be(ContextStatus.Killed)

      //cleanup
      daoActor ! JobDAOActor.SaveContextInfo(msg.contextInfo.get.copy(state = ContextStatus.Finished))
      expectMsg(JobDAOActor.SavedSuccessfully)
    }

  }
}
