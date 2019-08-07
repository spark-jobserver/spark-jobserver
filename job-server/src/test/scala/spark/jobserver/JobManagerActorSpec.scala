package spark.jobserver

import java.io.File
import java.nio.file.{Files, StandardOpenOption}

import akka.actor.{ActorIdentity, ActorRef, Cancellable, PoisonPill, Props, ReceiveTimeout}
import akka.testkit.TestProbe
import org.joda.time.DateTime
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory
import spark.jobserver.DataManagerActor.RetrieveData
import spark.jobserver.JobManagerActor.{Initialize, KillJob}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.CommonMessages.{JobRestartFailed, JobStarted, JobValidationFailed, Subscribe}
import spark.jobserver.io._
import spark.jobserver.ContextSupervisor.{ContextStopError, ContextStopInProgress, SparkContextStopped}
import spark.jobserver.io.JobDAOActor.SavedSuccessfully
import spark.jobserver.util._

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
  * SparkContext is designed to run only 1 instance per JVM due to extensive sharing and caching going on
  * but in test setups spark allows to have multiple contexts if spark.driver.allowMultipleContexts=true.
  * Jobserver is setting this property to true.
  *
  * The problem occurs when 1 context is shutting down and in parallel another one is coming up.
  * Both threads are using a static class SparkEnv. The thread which is shutting down is executing
  * SparkEnv.set(null) and the thread which is coming up is trying to do SparkEnv.get,
  * if the timing is correct then we get a NullPointerException and tests become flaky.
  *
  * The reason why both threads are doing this in parallel is that Spark reports a bit early that
  * context has stopped (onApplicationEnd event) and then performs the cleanup (SparkEnv.set(null))
  * and since Jobserver relies on this event, it moves to the next testcase and starts a new context.
  *
  * The idea is to not rely on onApplicationEnd and call stop synchronously so we can be sure that the
  * next testcase will get a clean SparkEnv. Further, if context is already stopped we check if SparkEnv
  * is null or not, if not we retry upto 10 times.
  *
  * Any extension of JobManagerActor for test purposes should extend this trait.
  */
abstract class CleanlyStoppingSparkContextJobManagerActor(daoActor: ActorRef, supervisorActorAddress: String,
    contextId: String, initializationTimeout: FiniteDuration)
  extends JobManagerActor(daoActor, supervisorActorAddress, contextId, initializationTimeout) {

  override def wrappedReceive: Receive = {
    this.cleanSparkContextReceive.orElse(super.wrappedReceive)
  }

  def cleanSparkContextReceive: Receive = {
    case "CleanSparkContext" => {
      stopContextSynchronously(jobContext) match {
        case true =>
          logger.debug("Cleaned the context")
          sender ! "Cleaned"
        case false =>
          logger.debug("Failed to clean the context")
          sender ! "FailedToClean"
      }
    }
  }

  def stopContextSynchronously(context: ContextLike): Boolean = {
    logger.debug("Trying to stop spark context")
    Option(context) match {
      case None =>
        logger.debug("Context is already stopped, checking SparkEnv ...")
        waitForSparkEnvToBeNull()
        true
      case Some(context) =>
        context.stop()
        logger.debug(s"Spark context stopped. SparkEnv is ${SparkEnv.get}")
        true
    }
  }

  def waitForSparkEnvToBeNull(): Unit = {
    logger.debug("Starting to do multiple retries for SparkEnv = null")
    Utils.retry(10, 500) {
      if (SparkEnv.get != null) {
        logger.debug(s"SparkEnv is still not null, retrying .... ${SparkEnv.get}")
        throw new Exception("Spark Env is still not null")
      }
    }
    logger.debug("SparkEnv is null")
  }
}

object JobManagerActorSpec extends JobSpecConfig

object JobManagerActorSpyStateUpdate {
  def props(daoActor: ActorRef, supervisorActorAddress: String,
            initializationTimeout: FiniteDuration,
            contextId: String, spyActorRef: ActorRef): Props =
    Props(classOf[JobManagerActorSpyStateUpdate], daoActor, supervisorActorAddress,
      initializationTimeout, contextId, spyActorRef)
}

class JobManagerActorSpyStateUpdate(daoActor: ActorRef, supervisorActorAddress: String,
                         initializationTimeout: FiniteDuration, contextId: String, spyActorRef: ActorRef)
    extends CleanlyStoppingSparkContextJobManagerActor(
      daoActor, supervisorActorAddress, contextId, initializationTimeout) {

  override protected def getUpdateContextByIdFuture(contextId: String,
        attributes: ContextModifiableAttributes): Future[JobDAOActor.SaveResponse] = {
    super.getUpdateContextByIdFuture(contextId, attributes).andThen {
      case _ => spyActorRef ! "Save Complete"
    }
  }
}

object JobManagerActorSpy {
  case object TriggerExternalKill
  case object UnhandledException
  case object GetStatusActorRef

  def props(daoActor: ActorRef, supervisorActorAddress: String,
      initializationTimeout: FiniteDuration,
      contextId: String, spyProbe: TestProbe, restartCandidates: Int = 0): Props =
    Props(classOf[JobManagerActorSpy], daoActor, supervisorActorAddress,
        initializationTimeout, contextId, spyProbe, restartCandidates)
}
class JobManagerActorSpy(daoActor: ActorRef, supervisorActorAddress: String,
    initializationTimeout: FiniteDuration, contextId: String, spyProbe: TestProbe,
    restartCandidates: Int = 0)
    extends CleanlyStoppingSparkContextJobManagerActor(
      daoActor, supervisorActorAddress, contextId, initializationTimeout) {

  totalJobsToRestart = restartCandidates

  def stubbedWrappedReceive: Receive = {
    case msg @ JobStarted(jobId, jobInfo) =>
      spyProbe.ref ! msg

    case msg @ JobRestartFailed(jobId, err) => {
      handleJobRestartFailure(jobId, err, msg)
      spyProbe.ref ! msg
    }

    case msg @ JobValidationFailed(jobId, dateTime, err) => {
      handleJobRestartFailure(jobId, err, msg)
      spyProbe.ref ! msg
    }

    case JobManagerActorSpy.TriggerExternalKill =>
      jobContext.stop()
      sender ! "Stopped"

    case JobManagerActorSpy.UnhandledException => throw new Exception("I am unhandled")

    case JobManagerActorSpy.GetStatusActorRef => sender ! statusActor
  }

  override def stoppingStateReceive: Receive =
    super.cleanSparkContextReceive.orElse(super.stoppingStateReceive)

  override def wrappedReceive: Receive =
    this.stubbedWrappedReceive.orElse(super.wrappedReceive)

  override protected def sendStartJobMessage(receiverActor: ActorRef, msg: JobManagerActor.StartJob) {
    spyProbe.ref ! "StartJob Received"
    receiverActor ! msg
  }

  override protected def forcefulKillCaller(forcefulKill: StandaloneForcefulKill) = {
    contextId match {
      case "forceful_exception" =>
        throw new NotStandaloneModeException()
      case _ => jobContext.stop()
    }
  }

  override protected def scheduleContextStopTimeoutMsg(sender: ActorRef): Option[Cancellable] = {
    contextId match {
      case "normal_then_forceful" =>
        sender ! ContextStopInProgress
        None
      case _ => super.scheduleContextStopTimeoutMsg(sender)
    }
  }
}

object JobManagerTestActor {
  def props(daoActor: ActorRef, supervisorActorAddress: String = "", contextId: String = "",
            initializationTimeout: FiniteDuration = 40.seconds): Props =
    Props(classOf[JobManagerTestActor], daoActor, supervisorActorAddress, contextId, initializationTimeout)
}
class JobManagerTestActor(daoActor: ActorRef,
                          supervisorActorAddress: String,
                          contextId: String,
                          initializationTimeout: FiniteDuration) extends
  CleanlyStoppingSparkContextJobManagerActor(daoActor: ActorRef,
      supervisorActorAddress: String,
      contextId: String,
      initializationTimeout: FiniteDuration) {

  override def wrappedReceive: Receive = {
    this.initializeReceive.orElse(super.wrappedReceive)
  }

  /**
    * There are some testcases where the test is checking if the "manager" is terminated.
    * Since, jobserver relies on onApplicationEnd to find out if context is stopped or not,
    * it can happen that we move to the next testcase and SparkEnv is not clean. This can lead
    * to unwanted flakiness in the tests.
    *
    * The following override makes sure that before starting a context, SparkEnv is null.
    */
  def initializeReceive: Receive = {
    case msg @ Initialize(ctxConfig, resOpt, dataManagerActor) =>
      logger.debug("""
                   |Checking if SparkEnv is null. This is important because non-null
                   |SparkEnv can cause tests to fails with Null Pointer Exception""".stripMargin)
      waitForSparkEnvToBeNull()
      super.wrappedReceive(msg)
  }
}

class JobManagerActorSpec extends JobSpecBase(JobManagerActorSpec.getNewSystem) {
  import akka.testkit._
  import CommonMessages._
  import JobManagerActorSpec.MaxJobsPerContext
  import scala.concurrent.duration._

  val logger = LoggerFactory.getLogger(getClass)
  val classPrefix = "spark.jobserver."
  private val wordCountClass = classPrefix + "WordCountExample"
  private val newWordCountClass = classPrefix + "WordCountExampleNewApi"
  private val javaJob = classPrefix + "JavaHelloWorldJob"
  val sentence = "The lazy dog jumped over the fish"
  val counts = sentence.split(" ").groupBy(x => x).mapValues(_.length)
  protected val stringConfig = ConfigFactory.parseString(s"input.string = $sentence")
  protected val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val contextId = java.util.UUID.randomUUID().toString()

  val initMsgWait = 10.seconds.dilated
  val startJobWait = 5.seconds.dilated

  var contextConfig: Config = _

  before {
    logger.debug("Before block - started")
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    contextConfig = JobManagerActorSpec.getContextConfig(adhoc = false)
    manager = system.actorOf(JobManagerTestActor.props(daoActor, "", contextId, 40.seconds))
    logger.debug("Before block - finished")
  }

  after {
    logger.debug("After block - started")
    stopSparkContextIfAlive()
    AkkaTestUtils.shutdownAndWait(manager)
    Option(supervisor).foreach(AkkaTestUtils.shutdownAndWait(_))
    logger.debug("After block - finished")
  }

  private def stopSparkContextIfAlive(): Unit = {
    Try(Some(Await.result(
      system.actorSelection(manager.path).resolveOne()(5.seconds), 5.seconds))).getOrElse(None) match {
      case None => logger.debug("Manager is not alive")
      case _: Some[ActorRef] =>
        manager ! "CleanSparkContext"
        expectMsg(3.seconds, "Cleaned")
    }
  }

  describe("starting jobs") {
    it("should start job and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job and return result before job finish event") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgClass(classOf[JobResult])
      expectMsgClass(classOf[JobFinished])
      expectNoMsg()
    }

    it("should start job more than one time and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()

      // should be ok to run the same more again
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMsg()
    }

    it("should start job and return results (sync route) and the context should not terminate") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMsg(1.seconds)
    }

    it("should start job and return results and the context should not terminate " +
        s"if ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true because no error was reported") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      val ctxConfig = contextConfig.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))

      manager ! JobManagerActor.Initialize(ctxConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMsg(1.seconds)
    }

    it("should start multiple jobs and only terminate if an error " +
        s"was reported and ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      val ctxConfig = contextConfig.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))

      manager ! JobManagerActor.Initialize(ctxConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMsg(1.seconds)

      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMsg(1.seconds)

      manager ! JobManagerActor.StartJob("demo", classPrefix + "MyErrorJob", emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])
      deathWatch.expectTerminated(manager, 3.seconds)
    }

    it("should start NewAPI job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", newWordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMsg()
    }

    it("should start job and return JobStarted (async)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, errorEvents ++ asyncEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectNoMsg()
    }

    it("should start job, return JobStarted (async) and write context id and status to DAO") {
      import scala.concurrent.Await
      import spark.jobserver.io.JobStatus

      val configWithCtxId = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(configWithCtxId, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()

      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, errorEvents ++ asyncEvents)

      expectMsgClass(startJobWait, classOf[JobStarted])
      val jobInfo = Await.result(dao.getJobInfosByContextId(contextId), 5.seconds)
      jobInfo should not be (None)
      jobInfo.length should be (1)
      jobInfo.head.contextId should be (contextId)
      jobInfo.head.state should be (JobStatus.Running)
      expectNoMsg()
    }

    it("should return error if job throws an error") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "MyErrorJob", emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])

      deathWatch.expectNoMsg(1.seconds)
    }

    it("should return error if job throws an error and " +
        s"stop context if ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR} is enabled") {
      contextConfig = contextConfig.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))

      val deathWatch = TestProbe()
      deathWatch.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()

      manager ! JobManagerActor.StartJob("demo", classPrefix + "MyErrorJob", emptyConfig, errorEvents)

      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])
      deathWatch.expectTerminated(manager, 2.seconds)
    }

    it("should return error if job throws a fatal error") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "MyFatalErrorJob", emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[OutOfMemoryError])
    }

    it("job should get jobConfig passed in to StartJob message") {
      val jobConfig = ConfigFactory.parseString("foo.bar.baz = 3")
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "ConfigCheckerJob", jobConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, keys: Seq[_]) =>
          keys should contain ("foo")
      }
    }

    it("should properly serialize case classes and other job jar classes") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "ZookeeperJob", stringConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Array[Product]) =>
          result.length should equal (1)
          result(0).getClass.getName should include ("Animal")
      }
      expectNoMsg()
    }

    it ("should refuse to start a job when too many jobs in the context are running") {
      val jobSleepTimeMillis = 2000L
      val jobConfig = ConfigFactory.parseString("sleep.time.millis = " + jobSleepTimeMillis)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()

      val messageCounts = new mutable.HashMap[Class[_], Int].withDefaultValue(0)
      // Try to start 3 instances of this job. 2 of them should start, and the 3rd should be denied.
      for (i <- 0 until MaxJobsPerContext + 1) {
        manager ! JobManagerActor.StartJob("demo", classPrefix + "SleepJob", jobConfig, allEvents)
      }

      while (messageCounts.values.sum < (MaxJobsPerContext * 3 + 1)) {
        expectMsgPF(5.seconds.dilated, "Expected a message but didn't get one!") {
          case started: JobStarted =>
            messageCounts(started.getClass) += 1
          case noSlots: NoJobSlotsAvailable =>
            noSlots.maxJobSlots should equal (MaxJobsPerContext)
            messageCounts(noSlots.getClass) += 1
          case finished: JobFinished =>
            messageCounts(finished.getClass) += 1
          case result: JobResult =>
            result.result should equal (jobSleepTimeMillis)
            messageCounts(result.getClass) += 1
        }
      }
      messageCounts.toMap should equal (Map(classOf[JobStarted] -> MaxJobsPerContext,
        classOf[JobFinished] -> MaxJobsPerContext,
        classOf[JobResult] -> MaxJobsPerContext,
        classOf[NoJobSlotsAvailable] -> 1))
    }

    it("should start a job that's an object rather than class") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Int) => result should equal (1 + 2 + 3)
      }
    }

    it("should be able to cancel running job") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "LongPiJob", stringConfig, allEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobStarted(id, _) =>
          manager ! KillJob(id)
          // we need this twice as we send both to sender and manager, in unit tests they are the same
          // in usage they may be different
          expectMsgClass(classOf[JobKilled])
          expectMsgClass(classOf[JobKilled])
      }
    }

    it("should fail a job that requires job jar dependencies but doesn't provide the jar"){
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      val jobJarDepsConfigs = ConfigFactory.parseString(
        s"""
           |dependent-jar-uris = []
        """.stripMargin)

      manager ! JobManagerActor.StartJob("demo", classPrefix + "jobJarDependenciesJob", jobJarDepsConfigs,
        syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobErroredOut])
    }

    it("should run a job that requires job jar dependencies if dependent-jar-uris are specified correctly"){
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      val jobJarDepsConfigs = ConfigFactory.parseString(
        s"""
           |dependent-jar-uris = ["file://${emptyJar.getAbsolutePath}"]
        """.stripMargin)

      manager ! JobManagerActor.StartJob("demo", classPrefix + "jobJarDependenciesJob", jobJarDepsConfigs,
        syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobResult])
    }

    it("should fail context creation if context has jar dependencies " +
      "but dependent-jar-uris are given as plain binary names"){
      val contextConfigWithDependentJar = contextConfig.withFallback(ConfigFactory.parseString(
        s"""
           |dependent-jar-uris = ["emptyjar"]
        """.stripMargin))
      manager ! JobManagerActor.Initialize(contextConfigWithDependentJar, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.InitError])
    }

    it("should create a context if context has jar dependencies " +
      "and dependent-jar-uris are provided in config file"){
      val contextConfigWithDependentJar = contextConfig.withFallback(ConfigFactory.parseString(
        s"""
           |dependent-jar-uris = ["file://${emptyJar.getAbsolutePath}"]
        """.stripMargin))
      manager ! JobManagerActor.Initialize(contextConfigWithDependentJar, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
    }

    it("jobs should be able to cache RDDs and retrieve them through getPersistentRDDs") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "CacheSomethingJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum: Int) = expectMsgClass(classOf[JobResult])

      manager ! JobManagerActor.StartJob("demo", classPrefix + "AccessCacheJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum2: Int) = expectMsgClass(classOf[JobResult])

      sum2 should equal (sum)
    }

    it ("jobs should be able to cache and retrieve RDDs by name") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "CacheRddByNameJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgPF(2 seconds, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, sum: Int) => sum should equal (1 + 4 + 9 + 16 + 25)
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
    }
  }

  describe("error conditions") {
    it("should return errors if appName does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo2", wordCountClass, emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchApplication)
    }

    it("should return error message if classPath does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should return error message if classPath does not match (adhoc)") {
      val adhocContextConfig = JobManagerActorSpec.getContextConfig(adhoc = true)
      val deathWatcher = TestProbe()
      uploadTestJar()

      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)
      manager ! JobManagerActor.Initialize(adhocContextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      deathWatcher.watch(manager)

      manager ! JobManagerActor.StartJob("demo", "no.such.class", emptyConfig, Set.empty[Class[_]])

      expectMsg(startJobWait, CommonMessages.NoSuchClass)
      deathWatcher.expectTerminated(manager)
      daoActor ! JobDAOActor.GetContextInfo(contextId)
      expectMsgPF(3.seconds, "") {
        case JobDAOActor.ContextResponse(Some(contextInfo)) =>
          contextInfo.state should be(ContextStatus.Stopping)
        case unexpectedMsg @ _ => fail(s"State is not stopping. Message received $unexpectedMsg")
      }
    }

    it("should error out if loading garbage jar") {
      uploadBinary(dao, "../README.md", "notajar", BinaryType.Jar)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("notajar", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should error out if job validation fails") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, emptyConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMsg()
    }

    it("should error out if new API job validation fails") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", newWordCountClass, emptyConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMsg()
    }
  }

  describe("kill-context-on-supervisor-down feature tests") {
    it("should not kill itself if kill-context-on-supervisor-down is disabled") {
      manager = system.actorOf(JobManagerTestActor.props(daoActor, "", contextId, 1.seconds.dilated))
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      managerProbe.expectNoMsg(2.seconds.dilated)
    }

    it("should kill itself if response to Identify message is not received when kill-context-on-supervisor-down is enabled") {
      manager = system.actorOf(JobManagerTestActor.props(daoActor, "fake-path", contextId, 1.seconds.dilated))
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      managerProbe.expectTerminated(manager, 2.seconds.dilated)
    }

    it("should kill itself if the master is down") {
      val dataManagerActor = system.actorOf(Props.empty)

      // A valid actor which responds to Identify message sent by JobManagerActor
      supervisor = system.actorOf(
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor), "context-supervisor")
      manager = system.actorOf(JobManagerTestActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}", contextId, 3.seconds.dilated))

      val supervisorProbe = TestProbe()
      supervisorProbe.watch(supervisor)

      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      Thread.sleep(2000) // Wait for manager actor to initialize and add a watch
      supervisor ! PoisonPill

      supervisorProbe.expectTerminated(supervisor)
      managerProbe.expectTerminated(manager, 4.seconds.dilated)
    }

    it("should kill itself if Initialize message is not received") {
      val dataManagerActor = system.actorOf(Props.empty)

      supervisor = system.actorOf(
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor), "context-supervisor")
      manager = system.actorOf(JobManagerTestActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}", contextId, 2.seconds.dilated))

      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      // supervisor not sending a message is equal to message not received in manager

      // Since, ReceiveTimeout and ActorIdentity are internal messages, there is no
      // direct way to verify them. We can only verify the effect that it gets killed.
      managerProbe.expectTerminated(manager, 3.seconds.dilated)
    }

    it("should not kill itself if Initialize message is received") {
      val dataManagerActor = system.actorOf(Props.empty)

      supervisor = system.actorOf(
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor), "context-supervisor")
      manager = system.actorOf(JobManagerTestActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}", contextId, 3.seconds.dilated))
      // Wait for Identify/ActorIdentify message exchange
      Thread.sleep(2000)
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      manager ! JobManagerActor.Initialize(contextConfig, None, TestProbe().ref)

      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      managerProbe.expectNoMsg(4.seconds.dilated)
    }
  }

  describe("remote file cache") {
    it("should support local copy, fetch from remote and cache file") {
      val testData = "test-data".getBytes
      val dataFileActor = TestProbe()

      manager ! JobManagerActor.Initialize(contextConfig, None, dataFileActor.ref)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()

      // use already existing file
      val existingFile = File.createTempFile("test-existing-file-", ".dat")
      Files.write(existingFile.toPath, testData, StandardOpenOption.SYNC)
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob",
        ConfigFactory.parseString(s"testFile = ${existingFile.getAbsolutePath}"),
        errorEvents ++ syncEvents)
      dataFileActor.expectNoMsg()
      val existingFileResult = expectMsgPF() {
        case JobResult(_, fileName: String) => fileName
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      existingFileResult should equal (existingFile.getAbsolutePath)

      // downloads new file from data file manager
      val jobConfig = ConfigFactory.parseString("testFile = test-file")
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob", jobConfig,
        errorEvents ++ syncEvents)
      dataFileActor.expectMsgPF() {
        case RetrieveData(fileName, jobManager) => {
          fileName should equal ("test-file")
          jobManager should equal (manager)
        }
      }
      dataFileActor.reply(DataManagerActor.Data("test-data".getBytes))
      val cachedFile = expectMsgPF() {
        case JobResult(_, fileName: String) => fileName
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      val cachedFilePath = new File(cachedFile).toPath
      Files.exists(cachedFilePath) should equal (true)
      Files.readAllBytes(cachedFilePath) should equal ("test-data".getBytes)

      // uses cached version in second run
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob", jobConfig,
        errorEvents ++ syncEvents)
      dataFileActor.expectNoMsg() // already cached
      val secondResult = expectMsgPF() {
        case JobResult(_, fileName: String) => fileName
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      cachedFile should equal (secondResult)

      manager ! JobManagerActor.DeleteData("test-file") // cleanup
    }

    it("should fail if remote data file does not exists") {
      val testData = "test-data".getBytes
      val dataFileActor = TestProbe()

      manager ! JobManagerActor.Initialize(contextConfig, None, dataFileActor.ref)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()

      val jobConfig = ConfigFactory.parseString("testFile = test-file")
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob", jobConfig,
        errorEvents ++ syncEvents)

      // return error from file manager
      dataFileActor.expectMsgClass(classOf[RetrieveData])
      dataFileActor.reply(DataManagerActor.Error("test"))

      expectMsgClass(classOf[JobErroredOut])
    }
  }

  describe("Supervise mode unit tests") {
    it("should kill itself if, the only restart candidate job failed") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, "", TestProbe(), 1))
      deathWatcher.watch(manager)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectTerminated(manager)
    }

    it("should kill itself if, all restart candidates failed") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, "", TestProbe(), 3))
      deathWatcher.watch(manager)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobValidationFailed("dummy-id1", DateTime.now(), new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobRestartFailed("dummy-id2", new Exception(""))
      deathWatcher.expectTerminated(manager)
    }

    it("should not kill itself if atleast one job was restarted") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, "", TestProbe(), 3))
      deathWatcher.watch(manager)

      val dummyJob = JobInfo("dummy-id2", "", "", BinaryInfo("dummy", BinaryType.Jar, DateTime.now()), "",
          JobStatus.Running, DateTime.now(), None, None)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobStarted("dummy-id2", dummyJob)
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobValidationFailed("dummy-id2", DateTime.now(), new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)
    }

    it("should not do anything if no context was found with specified name") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))
      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectNoMsg()
    }

    it("should kill itself if an exception occurred by accessing DAO") {
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, spyProbe))
      managerWatcher.watch(manager)

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      // Not replying to GetJobInfosByContextId will result in a timeout
      // and failure will occur
      val msg = daoProbe.expectMsgClass(4.seconds, classOf[JobDAOActor.UpdateContextById])
      msg.attributes.state should be(ContextStatus.Error)
      msg.attributes.error.get.getMessage should be(s"Failed to fetch jobs for context $contextId")
      daoProbe.reply(JobDAOActor.SavedSuccessfully)
      managerWatcher.expectTerminated(manager, 5.seconds)
    }

    it("should kill itself if an unexpected message is received from DAO") {
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, spyProbe))
      managerWatcher.watch(manager)

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply("Nobody gonna save you now!")
      managerWatcher.expectTerminated(manager)
    }

    it("should not do anything if context was found but no restart candidate jobs") {
      val contextId = "dummy-context"
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, spyProbe))

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply(JobDAOActor.JobInfos(Seq()))
      spyProbe.expectNoMsg()
    }

    it("should not restart a job if job config is not found in DAO") {
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val binaryInfo = BinaryInfo("dummy", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, "test-class", JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, TestProbe()))
      managerWatcher.watch(manager)
      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply(JobDAOActor.JobInfos(Seq(jobInfo)))

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobConfig])
      daoProbe.reply(JobDAOActor.JobConfig(None))

      daoProbe.expectMsgPF(3.seconds.dilated, "store error in DAO since config not found") {
        case jobInfo: JobDAOActor.SaveJobInfo =>
          jobInfo.jobInfo.state should be(JobStatus.Error)
          jobInfo.jobInfo.error.get.message should be((NoJobConfigFoundException("jobId")).getMessage)
      }

      val msg = daoProbe.expectMsgClass(classOf[JobDAOActor.UpdateContextById])
      msg.attributes.state should be(ContextStatus.Error)
      msg.attributes.error.get.getMessage() should be("Job(s) restart failed")
      daoProbe.reply(JobDAOActor.SavedSuccessfully)
      managerWatcher.expectTerminated(manager)
    }

    it("should not restart a job if there was an error while fetching job config") {
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val binaryInfo = BinaryInfo("dummy", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, "test-class", JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, TestProbe()))
      managerWatcher.watch(manager)
      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply(JobDAOActor.JobInfos(Seq(jobInfo)))

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobConfig])

      daoProbe.expectMsgPF(5.seconds.dilated, "store error in DAO since config not found") {
        case jobInfo: JobDAOActor.SaveJobInfo =>
          jobInfo.jobInfo.state should be(JobStatus.Error)
      }

      val msg = daoProbe.expectMsgClass(classOf[JobDAOActor.UpdateContextById])
      msg.attributes.state should be(ContextStatus.Error)
      msg.attributes.error.get.getMessage() should be("Job(s) restart failed")
      daoProbe.reply(JobDAOActor.SavedSuccessfully)
      managerWatcher.expectTerminated(manager)
    }
  }

  describe("Supervise mode scenarios") {
    it("should restart if running job was found with valid config") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMsg()
    }

    it("should restart if a job was found with valid config and state Restarting") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Restarting, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMsg()
    }

    it("should restart multiple jobs if available for a specific context") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId1", state = JobStatus.Restarting))
      dao.saveJobConfig("jobId1", stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsg("StartJob Received")

      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMsg()
    }

    it("should restart jobs only if they have status Running or Restarting") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val contextName = "context-name"
      val daoProbe = TestProbe()
      uploadTestJar("test-jar")
      val binaryInfo = dao.getBinaryInfo("test-jar").get
      val jobInfo = JobInfo("jobId", contextId, contextName,
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId,context.name=$contextName").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId1", state = JobStatus.Error))
      dao.saveJobConfig("jobId1", stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId2", state = JobStatus.Finished))
      dao.saveJobConfig("jobId2", stringConfig)
      val jobInfo3 = jobInfo.copy(jobId = "jobId3", state = JobStatus.Restarting)
      dao.saveJobInfo(jobInfo3)
      dao.saveJobConfig("jobId3", stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsg("StartJob Received")

      spyProbe.expectMsgAllOf(JobStarted(jobInfo.jobId, jobInfo),
          JobStarted(jobInfo3.jobId, jobInfo3.copy(state = JobStatus.Running)))
      spyProbe.expectNoMsg()
    }

    it("should keep the context JVM running if restart of atleast 1 job is successful") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val deathWatcher = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))
      deathWatcher.watch(manager)

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId1")) // Not saving config for jobId1 causes restart failure

      manager ! JobManagerActor.RestartExistingJobs

      val messages = spyProbe.expectMsgAllClassOf(
        classOf[String], classOf[JobStarted], classOf[JobRestartFailed])
      messages.filter(_.isInstanceOf[String]).head should be("StartJob Received")

      spyProbe.expectNoMsg()
      deathWatcher.expectNoMsg()
    }
  }

  describe("Context stop tests") {
    it("should stop context and shutdown itself") {
      val deathWatcher = TestProbe()
      deathWatcher.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StopContextAndShutdown
      expectMsg(SparkContextStopped)
      deathWatcher.expectTerminated(manager)
    }

    it("should stop context, kill status actor and shutdown itself (in order)") {
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, TestProbe(), 1))
      watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActorSpy.GetStatusActorRef
      val statusActorRef = expectMsgType[ActorRef]
      watch(statusActorRef)

      manager ! JobManagerActor.StopContextAndShutdown

      expectTerminated(statusActorRef)
      expectMsg(SparkContextStopped)
      expectTerminated(manager)
    }

    it("should update DAO if an external kill event is fired to stop the context") {
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, TestProbe(), 1))
      val deathWatcher = TestProbe()
      deathWatcher.watch(manager)

      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActorSpy.TriggerExternalKill

      expectMsg("Stopped")
      deathWatcher.expectTerminated(manager)
      daoActor ! JobDAOActor.GetContextInfo(contextId)
      expectMsgPF(3.seconds, "") {
        case JobDAOActor.ContextResponse(Some(contextInfo)) =>
          contextInfo.state should be(ContextStatus.Killed)
        case unexpectedMsg => fail(s"State is not killed")
      }
    }

    it("should stop context and cleanup properly in case of unhandled exception") {
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, TestProbe(), 1))
      val deathWatcher = TestProbe()
      deathWatcher.watch(manager)

      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActorSpy.UnhandledException

      deathWatcher.expectTerminated(manager, 5.seconds)
      daoActor ! JobDAOActor.GetContextInfo(contextId)
      expectMsgPF(3.seconds, "") {
        case JobDAOActor.ContextResponse(Some(contextInfo)) =>
          contextInfo.state should be(ContextStatus.Killed)
          contextInfo.endTime should not be(None)
        case unexpectedMsg => fail(s"State is not killed")
      }
    }

    it("should start a job in adhoc context and should stop context once finished") {
      manager = system.actorOf(JobManagerActorSpyStateUpdate.props(daoActor, "", 5.seconds, contextId, self))
      val adhocContextConfig = JobManagerActorSpec.getContextConfig(adhoc = true)
      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      manager ! JobManagerActor.Initialize(adhocContextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      watch(manager)

      uploadTestJar()

      // action
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)

      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectMsg("Save Complete") // State STOPPING has been updated
      expectTerminated(manager)

      daoActor ! JobDAOActor.GetContextInfo(contextId)
      expectMsgPF(3.seconds, "") {
        case JobDAOActor.ContextResponse(Some(contextInfo)) =>
          contextInfo.state should be(ContextStatus.Stopping)
          contextInfo.endTime should be(None)
        case unexpectedMsg @ _ => fail(s"State is not stopping. Message received $unexpectedMsg")
      }
      expectNoMsg(500.milliseconds)
    }
  }

  describe("Forceful context stop tests") {
    it("should stop context forcefully and shutdown itself") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, TestProbe(), 1))
      deathWatcher.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StopContextForcefully
      expectMsg(SparkContextStopped)
      deathWatcher.expectTerminated(manager)
    }

    it("should forcefully stop context, kill status actor and shutdown itself (in order)") {
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, TestProbe(), 1))
      watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActorSpy.GetStatusActorRef
      val statusActorRef = expectMsgType[ActorRef]
      watch(statusActorRef)

      manager ! JobManagerActor.StopContextForcefully

      expectTerminated(statusActorRef)
      expectMsg(SparkContextStopped)
      expectTerminated(manager)
    }

    it("should send ContextStopError in case of an exception for forceful context stop") {
      manager = system.actorOf(JobManagerActorSpy.props(
          daoActor, "", 5.seconds, "forceful_exception", TestProbe(), 1))
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StopContextForcefully
      expectMsg(ContextStopError(new NotStandaloneModeException))
    }

    it("should be able to stop context forcefully if normal stop timed out") {
      val deathWatcher = TestProbe()
      val contextId = "normal_then_forceful"
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, TestProbe(), 1))
      deathWatcher.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StopContextAndShutdown
      expectMsg(ContextStopInProgress)

      manager ! JobManagerActor.StopContextForcefully
      expectMsg(SparkContextStopped)
      deathWatcher.expectTerminated(manager)
    }
  }
}
