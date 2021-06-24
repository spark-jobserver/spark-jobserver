package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import akka.http.scaladsl.model.Uri
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory
import spark.jobserver.CommonMessages.{JobRestartFailed, JobStarted, JobValidationFailed}
import spark.jobserver.ContextSupervisor.{ContextStopError, ContextStopInProgress, SparkContextStopped}
import spark.jobserver.DataManagerActor.RetrieveData
import spark.jobserver.JobManagerActor.{Initialize, KillJob}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.JobDAOActor.{SaveJobConfig, SaveJobInfo, SavedSuccessfully}
import spark.jobserver.io._
import spark.jobserver.util._

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import java.time.ZonedDateTime
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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
  val logger = LoggerFactory.getLogger(getClass)

  def props(daoActor: ActorRef, supervisorActorAddress: String = "", contextId: String = "",
            initializationTimeout: FiniteDuration = 40.seconds,
            callbackHandler: CallbackHandler = new CallbackTestsHelper): Props =
    Props(classOf[JobManagerTestActor], daoActor, supervisorActorAddress, contextId, initializationTimeout,
      callbackHandler)

  def stopSparkContextIfAlive(system: ActorSystem, manager: ActorRef): Boolean = {
    val defaultSmallTimeout = 5.seconds

    Try(Some(Await.result(
      system.actorSelection(manager.path)
        .resolveOne()(defaultSmallTimeout), defaultSmallTimeout))).getOrElse(None) match {
      case None =>
        logger.debug("Manager is not alive")
        true
      case _: Some[ActorRef] =>
        Await.result((manager ? "CleanSparkContext")(defaultSmallTimeout), defaultSmallTimeout) match {
          case "Cleaned" => true
          case _ => false
        }
    }
  }
}
class JobManagerTestActor(daoActor: ActorRef,
                          supervisorActorAddress: String,
                          contextId: String,
                          initializationTimeout: FiniteDuration,
                          _callbackHandler: CallbackHandler) extends
  CleanlyStoppingSparkContextJobManagerActor(daoActor: ActorRef,
      supervisorActorAddress: String,
      contextId: String,
      initializationTimeout: FiniteDuration) {

  callbackHandler = _callbackHandler

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
  import CommonMessages._
  import JobManagerActorSpec.MaxJobsPerContext
  import akka.testkit._

  import scala.concurrent.duration._

  val logger = LoggerFactory.getLogger(getClass)
  val classPrefix = "spark.jobserver."
  private val wordCountClass = classPrefix + "WordCountExample"
  private val newWordCountClass = classPrefix + "WordCountExampleNewApi"
  private val javaJob = classPrefix + "JavaHelloWorldJob"
  val sentence = "The lazy dog jumped over the fish"
  val counts = sentence.split(" ").groupBy(x => x).mapValues(_.length)
  protected val baseJobConfig = ConfigFactory.parseString("cp = [\"demo\"]")
  protected val stringConfig = ConfigFactory.parseString(s"""
    |input.string = $sentence
    """.stripMargin).withFallback(baseJobConfig)
  protected val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val contextId = java.util.UUID.randomUUID().toString()

  implicit private val futureTimeout = Timeout(5.seconds)
  val initMsgWait = 10.seconds.dilated
  val startJobWait = 5.seconds.dilated
  val defaultSmallTimeout = 5.seconds

  var contextConfig: Config = _
  var callbackHandler: CallbackTestsHelper = new CallbackTestsHelper()

  before {
    logger.debug("Before block - started")
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryDAO
    daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
    contextConfig = JobManagerActorSpec.getContextConfig(adhoc = false)
    callbackHandler = new CallbackTestsHelper()
    manager = system.actorOf(JobManagerTestActor.props(daoActor, "", contextId,
      40.seconds, callbackHandler))
    logger.debug("Before block - finished")
  }

  after {
    logger.debug("After block - started")
    JobManagerTestActor.stopSparkContextIfAlive(system, manager) should be(true)
    AkkaTestUtils.shutdownAndWait(manager)
    Option(supervisor).foreach(AkkaTestUtils.shutdownAndWait(_))
    logger.debug("After block - finished")
  }

  describe("starting jobs") {
    it("should start job and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(wordCountClass, List(testJar), stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMessage()
    }

    it("should start job and return result before job finish event") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(wordCountClass, List(testJar), stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgClass(classOf[JobResult])
      expectMsgClass(classOf[JobFinished])
      expectNoMessage()
    }

    it("should start job more than one time and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(wordCountClass, List(testJar), stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMessage()

      // should be ok to run the same more again
      manager ! JobManagerActor.StartJob(wordCountClass, List(testJar), stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMessage()
    }

    it("should start job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar), stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMessage()
      assert(callbackHandler.failureCount == 0)
      assert(callbackHandler.successCount == 0)
    }

    it("should start job and return results (sync route) and the context should not terminate") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar), stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMessage(1.seconds)
    }

    it("should start job and return results and the context should not terminate " +
        s"if ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true because no error was reported") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      val ctxConfig = contextConfig.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))

      manager ! JobManagerActor.Initialize(ctxConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar), stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMessage(1.seconds)
    }

    it("should start multiple jobs and only terminate if an error " +
        s"was reported and ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      val ctxConfig = contextConfig.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))

      manager ! JobManagerActor.Initialize(ctxConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar), stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMessage(1.seconds)

      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar), stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      deathWatch.expectNoMessage(1.seconds)

      manager ! JobManagerActor.StartJob(classPrefix + "MyErrorJob", List(testJar), emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])
      deathWatch.expectTerminated(manager, 3.seconds)
    }

    it("should start NewAPI job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        newWordCountClass, List(testJar), stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMessage()
    }

    it("should start job and return JobStarted (async)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar), stringConfig, errorEvents ++ asyncEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectNoMessage()
    }

    it("should start job, return JobStarted (async) and write context id and status to DAO") {
      import spark.jobserver.io.JobStatus

      import scala.concurrent.Await

      val configWithCtxId = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(configWithCtxId, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      val testJar = uploadTestJar()

      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar), stringConfig, errorEvents ++ asyncEvents)

      expectMsgClass(startJobWait, classOf[JobStarted])
      assert(callbackHandler.failureCount == 0)
      assert(callbackHandler.successCount == 0)
      val jobInfo = Utils.retry(3) {
        Await.result(inMemoryMetaDAO.getJobsByContextId(contextId)
          // JobInfo not present in DB yet. Try again later
          .map(s => if (s.isEmpty) throw new AssertionError("Expected jobInfo to be non-empty") else s),
          defaultSmallTimeout)
      }
      jobInfo should not be (None)
      jobInfo.length should be (1)
      jobInfo.head.contextId should be (contextId)
      Set(JobStatus.Running, JobStatus.Finished) should contain (jobInfo.head.state)
      expectNoMessage()
      assert(callbackHandler.failureCount == 0)
      assert(callbackHandler.successCount == 0)
    }

    it("should start job, return JobStarted (async) and invoke callback") {
      import spark.jobserver.io.JobStatus

      import scala.concurrent.Await

      val configWithCtxId = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(configWithCtxId, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      val testJar = uploadTestJar()

      manager ! JobManagerActor.StartJob(wordCountClass, List(testJar), stringConfig,
        errorEvents ++ asyncEvents, None, Some(Uri("http://example.com/")))

      expectMsgClass(startJobWait, classOf[JobStarted])
      assert(callbackHandler.failureCount == 0)
      assert(callbackHandler.successCount == 0)
      val jobInfo = Utils.retry(3) {
        Await.result(inMemoryMetaDAO.getJobsByContextId(contextId)
          // JobInfo not present in DB yet. Try again later
          .map(s => if (s.isEmpty) throw new AssertionError("Expected jobInfo to be non-empty") else s),
          defaultSmallTimeout)
      }
      jobInfo should not be (None)
      jobInfo.length should be (1)
      jobInfo.head.contextId should be (contextId)
      Set(JobStatus.Running, JobStatus.Finished) should contain (jobInfo.head.state)
      expectNoMessage()
      assert(callbackHandler.failureCount == 0)
      assert(callbackHandler.successCount == 1)
    }

    it("should return error if job throws an error") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        classPrefix + "MyErrorJob", List(testJar), baseJobConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])

      deathWatch.expectNoMessage(1.seconds)
    }

    it("should invoke callback if async job throws an error") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(classPrefix + "MyErrorJob", List(testJar), baseJobConfig,
        errorEvents, None, Some(Uri("http://example.com/")))
      assert(callbackHandler.failureCount == 0)
      assert(callbackHandler.successCount == 0)

      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])

      deathWatch.expectNoMessage(1.seconds)
      assert(callbackHandler.failureCount == 1)
      assert(callbackHandler.successCount == 0)
    }

    it("should return error if job throws an error and " +
        s"stop context if ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR} is enabled") {
      contextConfig = contextConfig.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))

      val deathWatch = TestProbe()
      deathWatch.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      val testJar = uploadTestJar()

      manager ! JobManagerActor.StartJob(
        classPrefix + "MyErrorJob", List(testJar), baseJobConfig, errorEvents)

      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])
      deathWatch.expectTerminated(manager, 2.seconds)
    }

    it("should return error if job throws a fatal error") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        classPrefix + "MyFatalErrorJob", List(testJar), baseJobConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[OutOfMemoryError])
    }

    it("job should get jobConfig passed in to StartJob message") {
      val jobConfig = ConfigFactory.parseString("foo.bar.baz = 3").withFallback(baseJobConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(classPrefix + "ConfigCheckerJob", List(testJar), jobConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, keys: Seq[_]) =>
          keys should contain ("foo")
      }
    }

    it("should properly serialize case classes and other job jar classes") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(classPrefix + "ZookeeperJob", List(testJar), stringConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Array[Product]) =>
          result.length should equal (1)
          result(0).getClass.getName should include ("Animal")
      }
      expectNoMessage()
    }

    it ("should refuse to start a job when too many jobs in the context are running") {
      val jobSleepTimeMillis = 2000L
      val jobConfig = ConfigFactory.parseString("sleep.time.millis = " + jobSleepTimeMillis).
        withFallback(baseJobConfig)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()

      val messageCounts = new mutable.HashMap[Class[_], Int].withDefaultValue(0)
      // Try to start 3 instances of this job. 2 of them should start, and the 3rd should be denied.
      for (i <- 0 until MaxJobsPerContext + 1) {
        manager ! JobManagerActor.StartJob(classPrefix + "SleepJob", List(testJar), jobConfig, allEvents)
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

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(classPrefix + "SimpleObjectJob", List(testJar), baseJobConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Int) => result should equal (1 + 2 + 3)
      }
    }

    it("should be able to cancel running job") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(classPrefix + "LongPiJob", List(testJar), stringConfig, allEvents)
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

      val testJar = uploadTestJar()

      manager ! JobManagerActor.StartJob(
        classPrefix + "jobJarDependenciesJob", List(testJar), emptyConfig, syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobErroredOut])
    }

    it("should run a job that requires job jar dependencies if dependent-jar-uris are specified correctly"){
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()

      val binURI = BinaryInfo( // Additional dependency binary, which contains Empty class
        s"file://${emptyJar.getAbsolutePath}",
        BinaryType.URI,
        ZonedDateTime.now()
      )
      manager ! JobManagerActor.StartJob(
        classPrefix + "jobJarDependenciesJob", List(testJar, binURI), emptyConfig, syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobResult])
    }

    it("should run a job that requires job jar dependencies if dependencies " +
      "are provided as uploaded binaries names"){
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      val depJar = uploadBinary(emptyJar.getAbsolutePath, "emptyjar", BinaryType.Jar)

      manager ! JobManagerActor.StartJob(
        classPrefix + "jobJarDependenciesJob", List(testJar, depJar), emptyConfig, syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobResult])
    }


    it("should create context that has jar dependencies and they are given as plain binary names"){
      uploadBinary(emptyJar.getAbsolutePath, "emptyjar", BinaryType.Jar)
      val contextConfigWithDependentJar = contextConfig.withFallback(ConfigFactory.parseString(
        s"""
           |cp = ["emptyjar"]
        """.stripMargin))
      manager ! JobManagerActor.Initialize(contextConfigWithDependentJar, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
    }

    it("should create context that has jar dependencies and they are given as URIs"){
      val contextConfigWithDependentJar = contextConfig.withFallback(ConfigFactory.parseString(
        s"""
           |cp = ["file://${emptyJar.getAbsolutePath}"]
        """.stripMargin))
      manager ! JobManagerActor.Initialize(contextConfigWithDependentJar, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
    }

    it("should create context that has dependencies given as mix of URIs and jar names"){
      uploadBinary(emptyJar.getAbsolutePath, "emptyjar", BinaryType.Jar)
      val contextConfigWithDependentJar = contextConfig.withFallback(ConfigFactory.parseString(
        s"""
           |cp = ["emptyjar", "file://${emptyJar.getAbsolutePath}"]
        """.stripMargin))
      manager ! JobManagerActor.Initialize(contextConfigWithDependentJar, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
    }


    it("should run a job that requires job jar dependencies" +
      "if dependent jars are specified in the context config as cp") {
      val testJar = uploadTestJar()
      uploadBinary(emptyJar.getAbsolutePath, "emptyjar", BinaryType.Jar)

      val contextConfigWithDependentJar = contextConfig.withFallback(ConfigFactory.parseString(
        s"""
           |cp = ["emptyjar"]
        """.stripMargin))
      manager ! JobManagerActor.Initialize(contextConfigWithDependentJar, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StartJob(
        classPrefix + "jobJarDependenciesJob", List(testJar), emptyConfig, syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobResult])
    }

    it("should run a job that requires job jar dependencies" +
      "if dependent jars are specified in the context config as dependent-jar-uris") {
      val testJar = uploadTestJar()
      uploadBinary(emptyJar.getAbsolutePath, "emptyjar", BinaryType.Jar)

      val contextConfigWithDependentJar = contextConfig.withFallback(ConfigFactory.parseString(
        s"""
           |dependent-jar-uris = ["emptyjar"]
        """.stripMargin))
      manager ! JobManagerActor.Initialize(contextConfigWithDependentJar, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StartJob(
        classPrefix + "jobJarDependenciesJob", List(testJar), emptyConfig, syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobResult])
    }

    it("jobs should be able to cache RDDs and retrieve them through getPersistentRDDs") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(classPrefix + "CacheSomethingJob", List(testJar), baseJobConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum: Int) = expectMsgClass(classOf[JobResult])

      manager ! JobManagerActor.StartJob(classPrefix + "AccessCacheJob", List(testJar), baseJobConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum2: Int) = expectMsgClass(classOf[JobResult])

      sum2 should equal (sum)
    }

    it ("jobs should be able to cache and retrieve RDDs by name") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(classPrefix + "CacheRddByNameJob", List(testJar), baseJobConfig,
        errorEvents ++ syncEvents)
      expectMsgPF(2 seconds, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, sum: Int) => sum should equal (1 + 4 + 9 + 16 + 25)
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
    }
  }

  describe("error conditions") {
    it("should return errors if appName does not match") {
      val testJar = uploadTestJar()
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      val wrongBinInfo = testJar.copy(appName = "SomeFakeName")
      manager ! JobManagerActor.StartJob(
        wordCountClass, List(testJar, wrongBinInfo), emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchFile("SomeFakeName"))
    }

    it("should return error message if classPath does not match") {
      val testJar = uploadTestJar()
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("no.such.class", List(testJar), baseJobConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should return error message if classPath does not match (adhoc)") {
      val adhocContextConfig = JobManagerActorSpec.getContextConfig(adhoc = true)
      val deathWatcher = TestProbe()
      val testJar = uploadTestJar()

      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, ZonedDateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)
      manager ! JobManagerActor.Initialize(adhocContextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      deathWatcher.watch(manager)

      manager ! JobManagerActor.StartJob("no.such.class", List(testJar), baseJobConfig, Set.empty[Class[_]])

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
      val testJar = uploadBinary("../README.md", "notajar", BinaryType.Jar)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      val notAJarConfig = ConfigFactory.parseString("cp = [\"notajar\"]")
      manager ! JobManagerActor.StartJob("no.such.class", List(testJar), notAJarConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should error out if job validation fails") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(wordCountClass, List(testJar), baseJobConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMessage()
    }

    it("should error out if new API job validation fails") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      val testJar = uploadTestJar()
      manager ! JobManagerActor.StartJob(newWordCountClass, List(testJar), baseJobConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMessage()
    }
  }

  describe("kill-context-on-supervisor-down feature tests") {
    it("should not kill itself if kill-context-on-supervisor-down is disabled") {
      manager = system.actorOf(JobManagerTestActor.props(daoActor, "", contextId,
        1.seconds.dilated, callbackHandler))
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      managerProbe.expectNoMessage(2.seconds.dilated)
    }

    it("should kill itself if response to Identify message is not" +
      " received when kill-context-on-supervisor-down is enabled") {
      manager = system.actorOf(JobManagerTestActor.props(daoActor, "fake-path", contextId,
        1.seconds.dilated, callbackHandler))
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      managerProbe.expectTerminated(manager, 2.seconds.dilated)
    }

    it("should kill itself if the master is down") {
      val dataManagerActor = system.actorOf(Props.empty)

      // A valid actor which responds to Identify message sent by JobManagerActor
      supervisor = system.actorOf(
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor),
        "context-supervisor")
      manager = system.actorOf(
        JobManagerTestActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}",
          contextId, 3.seconds.dilated, callbackHandler)
      )

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
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor),
        "context-supervisor")
      manager = system.actorOf(JobManagerTestActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}",
        contextId, 2.seconds.dilated, callbackHandler))

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
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}", contextId,
        3.seconds.dilated, callbackHandler))
      // Wait for Identify/ActorIdentify message exchange
      Thread.sleep(2000)
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      manager ! JobManagerActor.Initialize(contextConfig, None, TestProbe().ref)

      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      managerProbe.expectNoMessage(4.seconds.dilated)
    }
  }

  describe("remote file cache") {
    it("should support local copy, fetch from remote and cache file") {
      val testData = "test-data".getBytes
      val dataFileActor = TestProbe()

      manager ! JobManagerActor.Initialize(contextConfig, None, dataFileActor.ref)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      val testJar = uploadTestJar()

      // use already existing file
      val existingFile = File.createTempFile("test-existing-file-", ".dat")
      Files.write(existingFile.toPath, testData, StandardOpenOption.SYNC)
      manager ! JobManagerActor.StartJob(classPrefix + "RemoteDataFileJob", List(testJar),
        ConfigFactory.parseString(s"testFile = ${existingFile.getAbsolutePath}").withFallback(baseJobConfig),
        errorEvents ++ syncEvents)
      dataFileActor.expectNoMessage()
      val existingFileResult = expectMsgPF() {
        case JobResult(_, fileName: String) => fileName
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      existingFileResult should equal (existingFile.getAbsolutePath)

      // downloads new file from data file manager
      val jobConfig = ConfigFactory.parseString("testFile = test-file").withFallback(baseJobConfig)
      manager ! JobManagerActor.StartJob(classPrefix + "RemoteDataFileJob", List(testJar), jobConfig,
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
      manager ! JobManagerActor.StartJob(classPrefix + "RemoteDataFileJob", List(testJar), jobConfig,
        errorEvents ++ syncEvents)
      dataFileActor.expectNoMessage() // already cached
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
      val testJar = uploadTestJar()

      val jobConfig = ConfigFactory.parseString("testFile = test-file").withFallback(baseJobConfig)
      manager ! JobManagerActor.StartJob(classPrefix + "RemoteDataFileJob", List(testJar), jobConfig,
        errorEvents ++ syncEvents)

      // return error from file manager
      dataFileActor.expectMsgClass(classOf[RetrieveData])
      dataFileActor.reply(DataManagerActor.Error(new RuntimeException("test")))

      expectMsgClass(classOf[JobErroredOut])
    }
  }

  describe("Supervise mode unit tests") {
    it("should kill itself if, the only restart candidate job failed") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, "", TestProbe(), 1))
      deathWatcher.watch(manager)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectTerminated(manager)
    }

    it("should kill itself if, all restart candidates failed") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, "", TestProbe(), 3))
      deathWatcher.watch(manager)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectNoMessage(2.seconds)

      manager ! JobValidationFailed("dummy-id1", ZonedDateTime.now(), new Exception(""))
      deathWatcher.expectNoMessage(2.seconds)

      manager ! JobRestartFailed("dummy-id2", new Exception(""))
      deathWatcher.expectTerminated(manager)
    }

    it("should not kill itself if atleast one job was restarted") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, "", TestProbe(), 3))
      deathWatcher.watch(manager)

      val dummyJob = JobInfo("dummy-id2", "", "", "", JobStatus.Running,
        ZonedDateTime.now(), None, None, Seq(BinaryInfo("dummy", BinaryType.Jar, ZonedDateTime.now())))

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectNoMessage(2.seconds)

      manager ! JobStarted("dummy-id2", dummyJob)
      deathWatcher.expectNoMessage(2.seconds)

      manager ! JobValidationFailed("dummy-id2", ZonedDateTime.now(), new Exception(""))
      deathWatcher.expectNoMessage(2.seconds)
    }

    it("should not do anything if no context was found with specified name") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))
      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectNoMessage()
    }

    it("should kill itself if an exception occurred by accessing DAO") {
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", defaultSmallTimeout, contextId, spyProbe))
      managerWatcher.watch(manager)

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      // Not replying to GetJobInfosByContextId will result in a timeout
      // and failure will occur
      val msg = daoProbe.expectMsgClass(4.seconds, classOf[JobDAOActor.UpdateContextById])
      msg.attributes.state should be(ContextStatus.Error)
      msg.attributes.error.get.getMessage should be(s"Failed to fetch jobs for context $contextId")
      daoProbe.reply(JobDAOActor.SavedSuccessfully)
      managerWatcher.expectTerminated(manager, defaultSmallTimeout)
    }

    it("should kill itself if an unexpected message is received from DAO") {
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", defaultSmallTimeout, contextId, spyProbe))
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
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", defaultSmallTimeout, contextId, spyProbe))

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply(JobDAOActor.JobInfos(Seq()))
      spyProbe.expectNoMessage()
    }

    it("should not restart a job if job config is not found in DAO") {
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val binaryInfo = BinaryInfo("dummy", BinaryType.Jar, ZonedDateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
        "test-class", JobStatus.Running, ZonedDateTime.now(), None, None, Seq(binaryInfo))
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", defaultSmallTimeout, contextId, TestProbe()))
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
      val binaryInfo = BinaryInfo("dummy", BinaryType.Jar, ZonedDateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
        "test-class", JobStatus.Running, ZonedDateTime.now(), None, None, Seq(binaryInfo))
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", defaultSmallTimeout, contextId, TestProbe()))
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
      val binaryInfo = uploadTestJar()
      val jobInfo = JobInfo("jobId", contextId, "context-name",
        wordCountClass, JobStatus.Running, ZonedDateTime.now(), None, None, Seq(binaryInfo))
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      Await.result(daoActor ? SaveJobInfo(jobInfo), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig(jobInfo.jobId, stringConfig), defaultSmallTimeout)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMessage()
    }


    it("should restart if running job was found with valid config, but no cp set (based on binaryInfo)") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val binaryInfo = uploadTestJar()
      val jobInfo = JobInfo("jobId", contextId, "context-name",
        wordCountClass, JobStatus.Running, ZonedDateTime.now(), None, None, Seq(binaryInfo))
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      Await.result(daoActor ? SaveJobInfo(jobInfo), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig(
        jobInfo.jobId, ConfigFactory.parseString("input.string = a b c d")), defaultSmallTimeout)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMessage()
    }

    it("should restart if a job was found with valid config and state Restarting") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val binaryInfo = uploadTestJar()
      val jobInfo = JobInfo("jobId", contextId, "context-name",
        wordCountClass, JobStatus.Restarting, ZonedDateTime.now(), None, None, Seq(binaryInfo))
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      Await.result(daoActor ? SaveJobInfo(jobInfo), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig(jobInfo.jobId, stringConfig), defaultSmallTimeout)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMessage()
    }

    it("should restart multiple jobs if available for a specific context") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val binaryInfo = uploadTestJar()
      val jobInfo = JobInfo("jobId", contextId, "context-name",
        wordCountClass, JobStatus.Running, ZonedDateTime.now(), None, None, Seq(binaryInfo))
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      Await.result(daoActor ? SaveJobInfo(jobInfo), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig(jobInfo.jobId, stringConfig), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobInfo(
        jobInfo.copy(jobId = "jobId1", state = JobStatus.Restarting)), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig("jobId1", stringConfig), defaultSmallTimeout)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsg("StartJob Received")

      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMessage()
    }

    it("should restart jobs only if they have status Running or Restarting") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val contextName = "context-name"
      val testJar = uploadTestJar("test-jar")
      val jobInfo = JobInfo("jobId", contextId, contextName,
        wordCountClass, JobStatus.Running, ZonedDateTime.now(), None, None, Seq(testJar))
      val testJarJobConfig = ConfigFactory.parseString("cp = [\"test-jar\"]").withFallback(stringConfig)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId,context.name=$contextName").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      Await.result(daoActor ? SaveJobInfo(jobInfo), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig(jobInfo.jobId, testJarJobConfig), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobInfo(
        jobInfo.copy(jobId = "jobId1", state = JobStatus.Error)), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig("jobId1", testJarJobConfig), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobInfo(
        jobInfo.copy(jobId = "jobId2", state = JobStatus.Finished)), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig("jobId2", testJarJobConfig), defaultSmallTimeout)
      val jobInfo3 = jobInfo.copy(jobId = "jobId3", state = JobStatus.Restarting)
      Await.result(daoActor ? SaveJobInfo(jobInfo3), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig("jobId3", testJarJobConfig), defaultSmallTimeout)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsg("StartJob Received")

      spyProbe.expectMsgPF(initMsgWait, "") {
        case JobStarted(jobId, _) => Seq(jobInfo.jobId, jobInfo3.jobId) should contain (jobId)
      }
      spyProbe.expectMsgPF(initMsgWait, "") {
        case JobStarted(jobId, _) => Seq(jobInfo.jobId, jobInfo3.jobId) should contain (jobId)
      }

      spyProbe.expectNoMessage()
    }

    it("should keep the context JVM running if restart of atleast 1 job is successful") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val deathWatcher = TestProbe()
      val binaryInfo = uploadTestJar()
      val jobInfo = JobInfo("jobId", contextId, "context-name",
        wordCountClass, JobStatus.Running, ZonedDateTime.now(), None, None, Seq(binaryInfo))
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))
      deathWatcher.watch(manager)

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      Await.result(daoActor ? SaveJobInfo(jobInfo), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig(jobInfo.jobId, stringConfig), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobInfo(jobInfo.copy(jobId = "jobId1")), defaultSmallTimeout)

      manager ! JobManagerActor.RestartExistingJobs

      val messages = spyProbe.expectMsgAllClassOf(
        classOf[String], classOf[JobStarted], classOf[JobRestartFailed])
      messages.filter(_.isInstanceOf[String]).head should be("StartJob Received")

      spyProbe.expectNoMessage()
      deathWatcher.expectNoMessage()
    }

    it("should send JobRestartFailed message if failed to find any binary info for restart of the job") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val contextName = "context-name"
      val jobInfo = JobInfo("jobId", contextId, contextName,
        wordCountClass, JobStatus.Running, ZonedDateTime.now(), None, None, Seq.empty)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId,context.name=$contextName").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      Await.result(daoActor ? SaveJobInfo(jobInfo), defaultSmallTimeout)
      Await.result(daoActor ? SaveJobConfig(jobInfo.jobId, stringConfig), defaultSmallTimeout)

      manager ! JobManagerActor.RestartExistingJobs

      val msg = spyProbe.expectMsgType[JobRestartFailed]
      msg.jobId should equal(jobInfo.jobId)
      msg.err.getMessage should startWith("No binary of cp info in JobInfo for the job")
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
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, TestProbe(), 1))
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
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, TestProbe(), 1))
      val deathWatcher = TestProbe()
      deathWatcher.watch(manager)

      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, ZonedDateTime.now(), None, ContextStatus.Running, None))
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
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, TestProbe(), 1))
      val deathWatcher = TestProbe()
      deathWatcher.watch(manager)

      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, ZonedDateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActorSpy.UnhandledException

      deathWatcher.expectTerminated(manager, defaultSmallTimeout)
      daoActor ! JobDAOActor.GetContextInfo(contextId)
      expectMsgPF(3.seconds, "") {
        case JobDAOActor.ContextResponse(Some(contextInfo)) =>
          contextInfo.state should be(ContextStatus.Killed)
          contextInfo.endTime should not be(None)
        case unexpectedMsg => fail(s"State is not killed")
      }
    }

    it("should start a job in adhoc context and should stop context once finished") {
      manager = system.actorOf(JobManagerActorSpyStateUpdate.props(daoActor, "", defaultSmallTimeout, contextId, self))
      val adhocContextConfig = JobManagerActorSpec.getContextConfig(adhoc = true)
      daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(contextId, "ctx", "",
        None, ZonedDateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      manager ! JobManagerActor.Initialize(adhocContextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      watch(manager)

      val testJar = uploadTestJar()

      // action
      manager ! JobManagerActor.StartJob(wordCountClass, List(testJar), stringConfig, allEvents)

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
      expectNoMessage(500.milliseconds)
    }
  }

  describe("Forceful context stop tests") {
    it("should stop context forcefully and shutdown itself") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, TestProbe(), 1))
      deathWatcher.watch(manager)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StopContextForcefully
      expectMsg(SparkContextStopped)
      deathWatcher.expectTerminated(manager)
    }

    it("should forcefully stop context, kill status actor and shutdown itself (in order)") {
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, TestProbe(), 1))
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
          daoActor, "", defaultSmallTimeout, "forceful_exception", TestProbe(), 1))
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StopContextForcefully
      expectMsg(ContextStopError(new NotStandaloneModeException))
    }

    it("should be able to stop context forcefully if normal stop timed out") {
      val deathWatcher = TestProbe()
      val contextId = "normal_then_forceful"
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", defaultSmallTimeout, contextId, TestProbe(), 1))
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
