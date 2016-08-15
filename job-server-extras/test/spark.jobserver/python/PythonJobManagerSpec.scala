package spark.jobserver.python

import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages.{JobErroredOut, JobResult}
import spark.jobserver.io.JobDAOActor
import spark.jobserver._
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object PythonJobManagerSpec extends JobSpecConfig {
  override val contextFactory = classOf[PythonSparkContextFactory].getName
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
          |context-factory = "spark.jobserver.python.PythonSparkContextFactory"
          |context.name = "python_ctx"
          |context.actorName = "python_ctx"
        """.stripMargin).
        withFallback(PythonSparkContextFactorySpec.config)
      manager = system.actorOf(JobManagerActor.props(pyContextConfig, daoActor))

      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestEgg("python-demo")

      manager ! JobManagerActor.StartJob(
        "python-demo",
        "example_jobs.word_count.WordCountSparkJob",
        ConfigFactory.parseString("""input.strings = ["a", "b", "a"]"""),
        errorEvents ++ syncEvents)
      expectMsgPF(3 seconds, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, x) => x should matchPattern {
          case m: java.util.Map[_, _] if m.asScala == Map("b" -> 1, "a" -> 2) =>
        }
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
    }
  }
}
