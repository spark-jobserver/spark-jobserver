package spark.jobserver

import com.typesafe.config.ConfigFactory
import spark.jobserver.io.JobDAOActor
import scala.concurrent.duration._

/**
 * This is just to test that you cannot load a SqlJob into a normal job context.
 */
object ContextJobSpec extends JobSpecConfig

class ContextJobSpec extends JobSpecBase(ContextJobSpec.getNewSystem) with Serializable {


  val classPrefix = "spark.jobserver."
  private val sqlTestClass = classPrefix + "SqlLoaderJob"

  protected val emptyConfig = ConfigFactory.empty()

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(ContextJobSpec.getContextConfig(false)))
  }

  describe("error conditions") {
    it("should get WrongJobType if loading SQL job in a plain SparkContext context") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(6 seconds, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo", sqlTestClass, emptyConfig, errorEvents)
      expectMsg(CommonMessages.WrongJobType)
    }
  }
}
