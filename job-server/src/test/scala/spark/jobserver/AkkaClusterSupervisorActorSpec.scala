package spark.jobserver

import akka.actor._
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.testkit._
import akka.cluster.ClusterEvent.MemberEvent

import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.{JobDAO, JobDAOActor, ContextInfo, ContextStatus}
import ContextSupervisor._
import spark.jobserver.util.{ManagerLauncher, ContextJVMInitializationTimeout, SparkJobUtils}
import spark.jobserver.JobManagerActor._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory, ConfigRenderOptions}
import org.scalatest.{Matchers, FunSpec, BeforeAndAfter, BeforeAndAfterAll, FunSpecLike}
import org.joda.time.DateTime
import scala.concurrent.Await
import scala.reflect.ClassTag
import java.util.concurrent.TimeUnit

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
    }
    spark {
      master = "local[4]"
      driver.supervise = false
      temp-contexts {
        num-cpu-cores = 4           # Number of cores to allocate.  Required.
        memory-per-node = 512m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
      }
      jobserver.job-result-cache-size = 100
      jobserver.context-creation-timeout = 5 s
      jobserver.dao-timeout = 3 s
      context-per-jvm = true
      contexts {
        config-context {
          num-cpu-cores = 4
          memory-per-node = 512m
        }
      }
      context-settings {
        num-cpu-cores = 2
        memory-per-node = 512m
        context-init-timeout = 2 s
        forked-jvm-init-timeout = 5s
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

class StubbedAkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef, managerProbe: TestProbe, cluster: Cluster)
        extends AkkaClusterSupervisorActor(daoActor, dataManagerActor, cluster) {

  override def preStart(): Unit = {
    cluster.join(selfAddress)
    cluster.subscribe(self, classOf[MemberEvent])
  }

  var daoCommunicationDisabled = false
  def createSlaveClusterWithJobManager(contextName: String, contextConfig: Config): (Cluster, ActorRef) = {
    val managerConfig = ConfigFactory.parseString("akka.cluster.roles=[manager],akka.remote.netty.tcp.port=0").withFallback(config)
    val managerSystem = ActorSystem(AkkaClusterSupervisorActorSpec.ACTOR_SYSTEM_NAME, managerConfig)

    val stubbedJobManagerRef = managerSystem.actorOf(Props(classOf[StubbedJobManagerActor], contextConfig), contextName)
    val cluster = Cluster(managerSystem)
    managerProbe.watch(stubbedJobManagerRef)
    (cluster, stubbedJobManagerRef)
  }

  override protected def launchDriver(name: String, contextConfig: Config, contextActorName: String): (Boolean, String) = {
    // Create probe and cluster and join back the master
    val shouldDriverLaunchFail = Try(contextConfig.getBoolean("driver.fail")).getOrElse(false)
    val shouldDriverFailToJoinCluster = Try(contextConfig.getBoolean("driver.cluster.join.fail")).getOrElse(false)
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
    super.wrappedReceive orElse(stubbedWrappedReceive)
  }

  def stubbedWrappedReceive: Receive = {
    case StubbedAkkaClusterSupervisorActor.AddContextToContextInitInfos(name) =>
      contextInitInfos(name) = ({ref=>}, {ref=>}, new Cancellable {
        def cancel(): Boolean = { return false }
        def isCancelled: Boolean = { return false }
      })
    case StubbedAkkaClusterSupervisorActor.DisableDAOCommunication =>
      daoCommunicationDisabled = true
    case StubbedAkkaClusterSupervisorActor.EnableDAOCommunication =>
      daoCommunicationDisabled = false
    case StubbedAkkaClusterSupervisorActor.DummyTerminated(actorRef) =>
      handleTerminatedEvent(actorRef)
  }

  override def getDataFromDAO[T: ClassTag](msg: JobDAOActor.JobDAORequest): Option[T] = {
    daoCommunicationDisabled match {
      case true => None
      case false => super.getDataFromDAO[T](msg)
    }
  }
}

class StubbedJobManagerActor(contextConfig: Config) extends Actor {
  def receive = {
    case JobManagerActor.Initialize(contextConfig,_,_) =>
      val resultActor = context.system.actorOf(Props(classOf[JobResultActor]))
      sender() ! JobManagerActor.Initialized(contextConfig.getString("context.name"), resultActor)
    case JobManagerActor.GetContexData =>
      val appId = Try(contextConfig.getString("manager.context.appId")).getOrElse("")
      val webUiUrl = Try(contextConfig.getString("manager.context.webUiUrl")).getOrElse("")
      (appId, webUiUrl) match {
        case ("", "") => sender() ! JobManagerActor.SparkContextDead
        case (_, "") => sender() ! JobManagerActor.ContexData(appId, None)
        case (_, _) => sender() ! JobManagerActor.ContexData(appId, Some(webUiUrl))
      }
  }
}

class AkkaClusterSupervisorActorSpec extends TestKit(AkkaClusterSupervisorActorSpec.system) with ImplicitSender
      with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  val daoTimeout = 5.seconds.dilated
  val contextInitTimeout = 10.seconds.dilated
  var supervisor: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _
  var managerProbe = TestProbe()
  val contextConfig = AkkaClusterSupervisorActorSpec.config.getConfig("spark.context-settings")
  val unusedDummyInput = 1

  // This is needed to help tests pass on some MBPs when working from home
  System.setProperty("spark.driver.host", "localhost")

  override def beforeAll() {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    val cluster = Cluster(system)
    supervisor = system.actorOf(Props(classOf[StubbedAkkaClusterSupervisorActor], daoActor, TestProbe().ref, managerProbe, cluster), "supervisor")
  }

  override def afterAll() = {
     AkkaTestUtils.shutdownAndWait(AkkaClusterSupervisorActorSpec.system)
  }

  after {
    // Cleanup all the context to have a fresh start for next testcase
    def stopContext(contextName: Any) {
      supervisor ! StopContext(contextName.toString())
      expectMsg(3.seconds.dilated, ContextStopped)
      managerProbe.expectMsgClass(classOf[Terminated])
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
        case (jobManagerActor: ActorRef) => true
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

    it("should not start two contexts with the same name") {
      supervisor ! AddContext("test-context3", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! AddContext("test-context3", contextConfig)
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
        case (manager: ActorRef) =>
          manager.path.name.startsWith("jobManager-")
      }

      isValid should be (true)

      supervisor ! ListContexts
      val hasContext = expectMsgPF(3.seconds.dilated) {
        case contexts: Seq[_] =>
          contexts.head.toString().endsWith("test-adhoc-classpath")
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
    }

    it("context stop should be able to handle case when no context is present") {
      supervisor ! StopContext("test-context5")
      expectMsg(NoSuchContext)
    }

    it("should be able to start multiple contexts") {
      supervisor ! AddContext("test-context6", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! AddContext("test-context7", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! ListContexts

      expectMsgAnyOf(Seq("test-context6", "test-context7"), Seq("test-context7", "test-context6"))
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
      val managerProbe = TestProbe(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX + "dummy")
      val contextId = managerProbe.ref.path.name.replace(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX, "")
      val deathWatch = TestProbe()
      deathWatch.watch(managerProbe.ref)
      val dummyContext = ContextInfo(contextId, "contextName", "", None,
          DateTime.now(), None, ContextStatus.Started, None)
      dao.saveContextInfo(dummyContext)

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))

      deathWatch.expectTerminated(managerProbe.ref)
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
      expectNoMsg((SparkJobUtils.getForkedJVMInitTimeout(system.settings.config) + 1).second)

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
      val timedOutRejoiningManagerProbe = TestProbe("jobManager-timedOutManager")
      val deathWatch = TestProbe()
      deathWatch.watch(timedOutRejoiningManagerProbe.ref)

      val contextActorName = timedOutRejoiningManagerProbe.ref.path.name
      supervisor ! StubbedAkkaClusterSupervisorActor.AddContextToContextInitInfos(contextActorName)

      val timedOutContextId = timedOutRejoiningManagerProbe.ref.path.name.replace(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX, "")
      val timedOutContext = ContextInfo(timedOutContextId, "contextName", "", None, DateTime.now(),
          Some(DateTime.now().plusHours(1)), ContextStatus.Error, Some(ContextJVMInitializationTimeout()))
      dao.saveContextInfo(timedOutContext)

      supervisor ! ActorIdentity(unusedDummyInput, Some(timedOutRejoiningManagerProbe.ref))

      deathWatch.expectTerminated(timedOutRejoiningManagerProbe.ref)
    }

    it("should kill context if it was marked as error in DAO and it is trying to join/initialize") {
      val erroredOutRejoiningManagerProbe = TestProbe("jobManager-erroredOutManager")
      val deathWatch = TestProbe()
      deathWatch.watch(erroredOutRejoiningManagerProbe.ref)

      val contextActorName = erroredOutRejoiningManagerProbe.ref.path.name
      supervisor ! StubbedAkkaClusterSupervisorActor.AddContextToContextInitInfos(contextActorName)

      val erroredOutContextId = erroredOutRejoiningManagerProbe.ref.path.name.replace(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX, "")
      val erroredOutContext = ContextInfo(erroredOutContextId, "contextName", "", None, DateTime.now(),
          Some(DateTime.now().plusHours(1)), ContextStatus.Error, Some(new Exception("random error")))
      dao.saveContextInfo(erroredOutContext)

      supervisor ! ActorIdentity(unusedDummyInput, Some(erroredOutRejoiningManagerProbe.ref))

      deathWatch.expectTerminated(erroredOutRejoiningManagerProbe.ref)
    }

    it("should not receive JVM creation timed out error if context was intialized properly") {
      supervisor ! AddContext("test-context-proper-init", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)
      expectNoMsg((SparkJobUtils.getForkedJVMInitTimeout(system.settings.config) + 1).second)
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

    it("should be able to list all the started contexts") {
      supervisor ! AddContext("test-context8", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! ListContexts
      expectMsg(Seq("test-context8"))
    }

    it("should return valid result actor") {
      supervisor ! AddContext("test-context9", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! GetResultActor("test-context9")
      expectMsgClass(classOf[ActorRef])
    }

    it("should return NoSuchContext if context is not available for GetSparkContextInfo") {
      supervisor ! GetSparkContexData("dummy-name")
      expectMsg(NoSuchContext)
    }

    it("should return valid appId and webUiUrl if context is running") {
      val configWithContextInfo = ConfigFactory.parseString("manager.context.webUiUrl=dummy-url,manager.context.appId=appId-dummy")
                .withFallback(contextConfig)
      supervisor ! AddContext("test-context10", configWithContextInfo)
      expectMsg(contextInitTimeout, ContextInitialized)

      val cont = Await.result(dao.getContextInfoByName("test-context10"), (3 seconds)).get

      supervisor ! GetSparkContexData("test-context10")
      expectMsg(SparkContexData(cont, Some("appId-dummy"), Some("dummy-url")))
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

      val contextInfoPF = ContextInfo(contextId, contextName, convertedContextConfig, None, DateTime.now(),
          None, _: String, None)
      val restartingContext = contextInfoPF(ContextStatus.Restarting)
      dao.saveContextInfo(restartingContext)

      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextAlreadyExists)

      dao.saveContextInfo(contextInfoPF(ContextStatus.Finished)) // cleanup
    }

    it("should return JVM initialization timeout if context JVM doesn't join cluster within timout") {
      val failingContextName = "test-context-cluster-fail-join"
      val wrongConfig = ConfigFactory.parseString("driver.cluster.join.fail=true").withFallback(contextConfig)

      supervisor ! AddContext(failingContextName, wrongConfig)

      val timeoutExceptionMessage = ContextJVMInitializationTimeout().getMessage
      val msg = expectMsgClass((SparkJobUtils.getForkedJVMInitTimeout(system.settings.config) + 1).seconds,
          classOf[ContextInitError])
      msg.t.getMessage should be(timeoutExceptionMessage)

      val timedOutContext = Await.result(dao.getContextInfoByName(failingContextName), daoTimeout)
      timedOutContext.get.state should be(ContextStatus.Error)
      timedOutContext.get.error.get.getMessage should be(timeoutExceptionMessage)
    }

    it("should not change final state to STOPPING state") {
      val contextId = "erroredOutContextId"
      val contextName = "erroredOutContextName"
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      val c = Await.result(dao.getContextInfoByName(contextName), daoTimeout).get
      val contextInfo = ContextInfo(c.id, contextName, c.config, c.actorAddress, c.startTime,
            Some(DateTime.now()), "ERROR", Some(new Throwable))
      dao.saveContextInfo(contextInfo)

      supervisor ! StopContext(contextName)
      expectMsg(contextInitTimeout, ContextStopped)

      val c_new = Await.result(dao.getContextInfoByName(contextName), daoTimeout).get
      c_new.state should be("ERROR")
    }

    it("should change non final state to STOPPING state") {
      val contextId = "runningContextId"
      val contextName = "runningContextName"
      supervisor ! AddContext(contextName, contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      val c = Await.result(dao.getContextInfoByName(contextName), daoTimeout).get
      val contextInfo = ContextInfo(c.id, contextName, c.config, c.actorAddress, c.startTime,
            Some(DateTime.now()), "RUNNING", Some(new Throwable))
      dao.saveContextInfo(contextInfo)

      supervisor ! StopContext(contextName)
      expectMsg(contextInitTimeout, ContextStopped)

      val c_new = Await.result(dao.getContextInfoByName(contextName), daoTimeout).get
      c_new.state should be("STOPPING")
    }
  }

  describe("Supervise mode tests") {
    it("should start context if supervise mode is disabled") {
      supervisor ! AddContext("test-context", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      val contextInfo = Await.result(dao.getContextInfoByName("test-context"), daoTimeout)
      contextInfo should not be (None)
    }

    it("should start adhoc context if supervise mode is disabled") {
      supervisor ! StartAdHocContext("test-adhoc-classpath", contextConfig)

      val isValid = expectMsgPF(contextInitTimeout, "manager and result actors") {
        case (manager: ActorRef) =>
          manager.path.name.startsWith("jobManager-")
      }

      isValid should be (true)
    }

    it("should start context with supervise mode enabled") {
      val configWithSuperviseMode = ConfigFactory.parseString(
          s"${ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY}=true").withFallback(contextConfig)
      supervisor ! AddContext("test-context", configWithSuperviseMode)
      expectMsg(contextInitTimeout, ContextInitialized)

      val contextInfo = Await.result(dao.getContextInfoByName("test-context"), daoTimeout)
      contextInfo should not be (None)
    }

    it("should try to restart context if supervise mode is enabled") {
      val managerProbe = TestProbe(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX + "123")
      managerProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case Initialize(_,_,_) => sender ! Initialized("", TestProbe().ref)
            case RestartExistingJobs =>
          }
          TestActor.KeepRunning
        }
      })

      val contextId = managerProbe.ref.path.name.replace(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX, "")
      val configWithSuperviseMode = ConfigFactory.parseString(
          s"${ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY}=true, is-adhoc=false, context.name=name, context.id=$contextId")
          .withFallback(contextConfig)
      val convertedContextConfig = configWithSuperviseMode.root().render(ConfigRenderOptions.concise())
      val restartedContext = ContextInfo(contextId, "", convertedContextConfig, None, DateTime.now(), None, ContextStatus.Started, None)
      dao.saveContextInfo(restartedContext)

      supervisor ! ActorIdentity(unusedDummyInput, Some(managerProbe.ref))

      managerProbe.expectMsgClass(classOf[Initialize])
      managerProbe.expectMsg(RestartExistingJobs)

      // After restart the context status is RUNNING. The after{} block of this class,
      // lists all contexts and then tries to stop them. Since this manager slave is just a
      // TestProbe it's address doesn't get resolved so, it cannot be stopped. So, we change
      // the status to finish to cleanup.
      dao.saveContextInfo(restartedContext.copy(state = ContextStatus.Finished))
    }

    it("should set state restarting for context which was terminated unexpectedly and had supervise mode enabled") {
      val contextId = "testid"
      val configWithSuperviseMode = ConfigFactory.parseString(
          s"${ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY}=true").withFallback(contextConfig)
      val convertedContextConfig = configWithSuperviseMode.root().render(ConfigRenderOptions.concise())

      val runningContext = ContextInfo(contextId, "c", convertedContextConfig, None, DateTime.now(), None, ContextStatus.Running, None)
      dao.saveContextInfo(runningContext)
      val managerProbe = system.actorOf(Props.empty, s"jobManager-$contextId")
      val daoProbe = TestProbe()

      supervisor ! StubbedAkkaClusterSupervisorActor.DummyTerminated(managerProbe)

      Thread.sleep(3000)
      val updatedContextInfo = Await.result(dao.getContextInfo(contextId), daoTimeout)
      updatedContextInfo.get.state should be(ContextStatus.Restarting)

      dao.saveContextInfo(updatedContextInfo.get.copy(state = ContextStatus.Finished)) //cleanup
    }

    it("should set state killed for context which was terminated unexpectedly and supervise mode disabled") {
      val contextId = "testid2"
      val convertedContextConfig = contextConfig.root().render(ConfigRenderOptions.concise())

      val runningContext = ContextInfo(contextId, "c", convertedContextConfig, None, DateTime.now(), None, ContextStatus.Running, None)
      dao.saveContextInfo(runningContext)
      val managerProbe = system.actorOf(Props.empty, s"jobManager-$contextId")
      val daoProbe = TestProbe()

      supervisor ! StubbedAkkaClusterSupervisorActor.DummyTerminated(managerProbe)

      Thread.sleep(3000)
      val updatedContextInfo = Await.result(dao.getContextInfo(contextId), daoTimeout)
      updatedContextInfo.get.state should be(ContextStatus.Killed)

      dao.saveContextInfo(updatedContextInfo.get.copy(state = ContextStatus.Finished)) //cleanup
    }
  }
}
