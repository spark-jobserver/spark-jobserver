package spark.jobserver

import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import spark.jobserver.CommonMessages._
import spark.jobserver.context.SQLContextFactory
import spark.jobserver.io.JobDAOActor
import scala.concurrent.duration._

object SqlJobSpec extends JobSpecConfig {
  override val contextFactory = classOf[SQLContextFactory].getName
}

class SqlJobSpec extends ExtrasJobSpecBase(SqlJobSpec.getNewSystem) {


  val classPrefix = "spark.jobserver."
  private val sqlLoaderClass = classPrefix + "SqlLoaderJob"
  private val sqlQueryClass = classPrefix + "SqlTestJob"

  val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val queryConfig = ConfigFactory.parseString(
                      """sql = "SELECT firstName, lastName FROM addresses WHERE city = 'San Jose'" """)

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
      SqlJobSpec.getContextConfig(false, SqlJobSpec.contextConfig),
      daoActor))
    supervisor = TestProbe().ref
  }

  describe("Spark SQL Jobs") {
    it("should be able to create and cache a table, then query it using separate SQL jobs") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", sqlLoaderClass, emptyConfig, syncEvents ++ errorEvents)
      expectMsgPF(6 seconds, "Did not get JobResult") {
        case JobResult(_, result: Long) => result should equal (3L)
      }
      expectNoMsg()

      manager ! JobManagerActor.StartJob("demo", sqlQueryClass, queryConfig, syncEvents ++ errorEvents)
      expectMsgPF(6 seconds, "Did not get JobResult") {
        case JobResult(_, result: Array[Row]) =>
          result should have length 2
          result(0)(0) should equal ("Bob")
      }
      expectNoMsg()

    }
  }
}
