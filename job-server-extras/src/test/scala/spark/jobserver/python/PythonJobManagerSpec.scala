package spark.jobserver.python

import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages.{JobErroredOut, JobResult}
import spark.jobserver.JobManagerActor.JobLoadingError
import spark.jobserver._
import spark.jobserver.io.{BinaryInfo, InMemoryBinaryDAO, InMemoryMetaDAO, JobDAOActor}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object PythonJobManagerSpec extends JobSpecConfig {
  override val contextFactory = classOf[PythonSessionContextFactory].getName
}

class PythonJobManagerSpec extends ExtrasJobSpecBase(PythonJobManagerSpec.getNewSystem) {

  before {
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryDAO
    daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
  }

  describe("PythonContextFactory used with JobManager") {
    def runJob(testBinInfo: BinaryInfo): Unit = {
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

    it("should execute eggs") {
      val testBinInfo = uploadTestEgg("python-demo")
      runJob(testBinInfo)
    }

    it("should execute wheels") {
      val testBinInfo = uploadTestWheel("python-demo")
      runJob(testBinInfo)
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
          error.getMessage should be ("Python should have exactly one package file! Found: 2")
      }
      expectNoMsg()
    }
  }
}
