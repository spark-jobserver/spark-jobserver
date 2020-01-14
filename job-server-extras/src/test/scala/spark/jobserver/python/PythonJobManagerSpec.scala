package spark.jobserver.python

import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages.{JobErroredOut, JobResult}
import spark.jobserver.io.JobDAOActor
import spark.jobserver._
import org.scalatest._
import spark.jobserver.JobManagerActor.JobLoadingError

import scala.concurrent.duration._
import scala.collection.JavaConverters._

object PythonJobManagerSpec extends JobSpecConfig {
  override val contextFactory = classOf[PythonSessionContextFactory].getName
}

class PythonJobManagerSpec extends ExtrasJobSpecBase(PythonJobManagerSpec.getNewSystem) {

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
  }

  describe("PythonContextFactory used with JobManager") {
    it("should work with JobManagerActor") {
      val pyContextConfig = ConfigFactory.parseString(
        """
          |context-factory = "spark.jobserver.python.TestPythonSessionContextFactory"
          |context.name = "python_ctx"
          |context.actorName = "python_ctx"
        """.stripMargin).
        withFallback(PythonSparkContextFactorySpec.config)
      manager = system.actorOf(JobManagerActor.props(daoActor))

      manager ! JobManagerActor.Initialize(pyContextConfig, None, emptyActor)
      expectMsgClass(30 seconds, classOf[JobManagerActor.Initialized])

      val testBinInfo = uploadTestEgg("python-demo")

      manager ! JobManagerActor.StartJob(
        "example_jobs.word_count.WordCountSparkSessionJob",
        Seq(testBinInfo),
        ConfigFactory.parseString("""input.strings = ["a", "b", "a"]"""),
        errorEvents ++ syncEvents)
      expectMsgPF(3 seconds, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, x) => x should matchPattern {
          case m: java.util.Map[_, _] if m.asScala == Map("b" -> 1, "a" -> 2) =>
        }
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      expectNoMsg()
    }

    it("should throw an error if job started from multiple binaries") {
      val pyContextConfig = ConfigFactory.parseString(
        """
          |context-factory = "spark.jobserver.python.TestPythonSessionContextFactory"
          |context.name = "python_ctx"
          |context.actorName = "python_ctx"
        """.stripMargin).
        withFallback(PythonSparkContextFactorySpec.config)
      manager = system.actorOf(JobManagerActor.props(daoActor))

      manager ! JobManagerActor.Initialize(pyContextConfig, None, emptyActor)
      expectMsgClass(30 seconds, classOf[JobManagerActor.Initialized])

      val testBinInfo = uploadTestEgg("python-demo")

      manager ! JobManagerActor.StartJob(
        "example_jobs.word_count.WordCountSparkSessionJob",
        Seq(testBinInfo, testBinInfo),
        ConfigFactory.parseString("""input.strings = ["a", "b", "a"]"""),
        errorEvents ++ syncEvents)
      expectMsgPF(3 seconds, "Expected a JobLoadingError message!") {
        case JobLoadingError(error: Throwable) =>
          error.getMessage should be ("Python should have exactly one egg file! Found: 2")
      }
      expectNoMsg()
    }
  }
}
