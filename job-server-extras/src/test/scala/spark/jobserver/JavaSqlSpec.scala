package spark.jobserver

import akka.actor._
import akka.testkit._
import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages._

import spark.jobserver.context.JavaSqlContextFactory
import spark.jobserver.io.JobDAOActor

import scala.concurrent.duration._

object JavaSqlSpec extends JobSpecConfig {
  override val contextFactory = classOf[JavaSqlContextFactory].getName
}

class JavaSqlSpec extends ExtrasJobSpecBase(JavaSqlSpec.getNewSystem) {

  private val emptyConfig = ConfigFactory.empty()
  private val classPrefix = "spark.jobserver."
  private val javaSqlClass = classPrefix + "JSqlTestJob"
  private def cfg = JavaSqlSpec.getContextConfig(false, JavaSqlSpec.contextConfig)

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(cfg, daoActor))
    supervisor = TestProbe().ref
  }

  describe("Running Java based SQLContext Jobs") {
    it("Should return Correct results") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", javaSqlClass, emptyConfig, syncEvents ++ errorEvents)
      expectMsgPF(2 seconds, "No?") {
        case JobResult(_, j: Int) =>
          j should equal(2)
      }
      expectNoMsg()

    }
  }
}
