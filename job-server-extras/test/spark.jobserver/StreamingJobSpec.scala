package spark.jobserver

import com.typesafe.config.ConfigFactory
import spark.jobserver.context.StreamingContextFactory
import spark.jobserver.io.{JobInfo, JobDAOActor}

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

  val configMap = Map("streaming.batch_interval" -> Integer.valueOf(3))

  val emptyConfig = ConfigFactory.parseMap(configMap.asJava)
  var jobId = ""

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
                             StreamingJobSpec.getContextConfig(false, StreamingJobSpec.contextConfig)))
  }

  describe("Spark Streaming Jobs") {
    it("should be able to process data using Streaming jobs") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(10 seconds, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", streamingJob, emptyConfig, asyncEvents ++ errorEvents)

      jobId = expectMsgPF(6 seconds, "Did not start StreamingTestJob, expecting JobStarted") {
        case JobStarted(jobid, _, _) => {
          jobid should not be null
          jobid
        }
      }
      Thread sleep 1000
      dao.getJobInfo(jobId).get match  {
        case JobInfo(_, _, _, _, _, None, _) => {  }
        case e => fail("Unexpected JobInfo" + e)
      }
    }
  }
}
