package spark.jobserver

import akka.pattern._
import akka.testkit.TestProbe

import scala.concurrent.Await
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import spark.jobserver.ContextSupervisor._
import spark.jobserver.JobManagerActor.{ContexData, GetContexData, StartJob, StopContextAndShutdown}
import spark.jobserver.context.StreamingContextFactory
import spark.jobserver.io.{JobDAOActor, JobInfo, JobStatus}
import spark.jobserver.util.{JobserverConfig, SparkJobUtils}

import scala.collection.mutable

/**
 * Test for Streaming Jobs.
 */
object StreamingJobSpec extends JobSpecConfig {
  override val contextFactory = classOf[StreamingContextFactory].getName
}

class StreamingJobSpec extends JobSpecBase(StreamingJobSpec.getNewSystem) {

  import CommonMessages._

  import collection.JavaConverters._
  import scala.concurrent.duration._

  val classPrefix = "spark.jobserver."
  private val streamingJob = classPrefix + "StreamingTestJob"
  private val failingStreamingJob = classPrefix + "StreamingTaskFailedTestJob"

  val configMap = Map("streaming.batch_interval" -> Integer.valueOf(3))

  val emptyConfig = ConfigFactory.parseMap(configMap.asJava)
  var jobId = ""
  val cfg = StreamingJobSpec.getContextConfig(false, StreamingJobSpec.contextConfig)
  val cfgWithGracefulShutdown = StreamingJobSpec.getContextConfig(
    false, StreamingJobSpec.contextConfigWithGracefulShutdown)

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(daoActor))
  }

  after {
    Await.result(gracefulStop(manager, 5 seconds), 5 seconds) // stop context
    // Due to some reason, even though the context is stopped, starting a new one gives the following error
    // org.apache.spark.SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243)
    // Thread.sleep() gives time to sc to stop fully
    Thread.sleep(3000)
  }

  describe("Spark Streaming Jobs") {
    it("should be able to process data using Streaming jobs and stop it") {
      val deathWatcher = TestProbe()
      deathWatcher.watch(manager)

      manager ! JobManagerActor.Initialize(cfg, None, emptyActor)
      expectMsgClass(10 seconds, classOf[JobManagerActor.Initialized])
      val testBinInfo = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        streamingJob, Seq(testBinInfo), emptyConfig, asyncEvents ++ errorEvents)

      jobId = expectMsgPF(6 seconds, "Did not start StreamingTestJob, expecting JobStarted") {
        case JobStarted(jobid, _) => {
          jobid should not be null
          jobid
        }
      }
      expectNoMsg(2.seconds)

      Thread sleep 1000
      val jobInfo = Await.result(dao.getJobInfo(jobId), 60 seconds)
      jobInfo.get match {
        case JobInfo(_, _, _, _, _, state, _, _, _, _) if state == JobStatus.Running => {  }
        case e => fail("Unexpected JobInfo" + e)
      }

      manager ! JobManagerActor.StopContextAndShutdown
      expectMsg(SparkContextStopped)
      deathWatcher.expectTerminated(manager)
    }

    it("should respond with stop in progress if stop times out and should eventually stop") {
      val deathWatcher = TestProbe()
      deathWatcher.watch(manager)
      val streamingJobConfig = ConfigFactory.parseString(
      """
        |streaming.test.job.maxIterations = 2,
        |streaming.test.job.totalDelaySeconds = 3,
        |streaming.test.job.printCount = true
      """.stripMargin.replace("\n", ""))

      manager ! JobManagerActor.Initialize(cfgWithGracefulShutdown, None, emptyActor)
      expectMsgClass(10 seconds, classOf[JobManagerActor.Initialized])
      val testBinInfo = uploadTestJar()

      manager ! JobManagerActor.StartJob(
        streamingJob, Seq(testBinInfo), streamingJobConfig, asyncEvents ++ errorEvents)
      expectMsgType[JobStarted]

      Thread.sleep(2000) // Allow the job to start processing data
      manager ! JobManagerActor.StopContextAndShutdown

      val expectedResponseTime =
          (SparkJobUtils.getContextDeletionTimeout(StreamingJobSpec.config) - 2) + 1
      expectMsg(expectedResponseTime.seconds, ContextStopInProgress)

      manager ! GetContexData
      expectMsgType[ContexData]

      manager ! JobManagerActor.StartJob(
        streamingJob, Seq(testBinInfo), streamingJobConfig, asyncEvents ++ errorEvents)
      expectMsg(ContextStopInProgress)

      manager ! StopContextAndShutdown
      expectMsg(ContextStopInProgress)

      expectMsg(9.seconds, SparkContextStopped)
      deathWatcher.expectTerminated(manager)
    }

    it("should stop a streaming context if job throws an exception " +
      s"and ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      val ctxConfig = cfg.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))
      triggerFailingStreamingJob(ctxConfig)

      deathWatch.expectTerminated(manager, 3.seconds)
    }

    it("should not stop a streaming context if job throws an " +
        s"exception and ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=false") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      val ctxConfig = cfg.withFallback(
        ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=false"))
      triggerFailingStreamingJob(ctxConfig)

      deathWatch.expectNoMsg(1.seconds)
    }

    it("should automatically stop a streaming context if" +
      s" job throws an exception (since ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR} is by default true)") {
      val deathWatch = TestProbe()
      deathWatch.watch(manager)

      triggerFailingStreamingJob(cfg)

      deathWatch.expectTerminated(manager, 3.seconds)
    }
  }

  private def triggerFailingStreamingJob(ctxConfig: Config): Unit = {
    manager ! JobManagerActor.Initialize(ctxConfig, None, emptyActor)
    expectMsgClass(10 seconds, classOf[JobManagerActor.Initialized])
    val testBinInfo = uploadTestJar()

    manager ! JobManagerActor.StartJob(
      failingStreamingJob, Seq(testBinInfo), emptyConfig, asyncEvents ++ errorEvents)

    expectMsgClass(2.seconds, classOf[JobStarted])
    expectMsgPF(6 seconds, "Expecting JobErrored Out") {
      case JobErroredOut(_, _, err) => {
        err.getMessage should include ("Job aborted due to stage failure")
      }
    }
  }
}
