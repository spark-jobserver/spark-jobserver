package spark.jobserver

import com.typesafe.config.{ConfigFactory}
import org.apache.spark.sql.Row
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.context.{HiveContextFactory}
import spark.jobserver.io.JobDAOActor

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.scalatest._


object HiveJobSpec extends JobSpecConfig {
  override val contextFactory = classOf[HiveContextFactory].getName
}

@Ignore
class HiveJobSpec extends ExtrasJobSpecBase(HiveJobSpec.getNewSystem) {

  val classPrefix = "spark.jobserver."
  private val hiveLoaderClass = classPrefix + "HiveLoaderJob"
  private val hiveQueryClass = classPrefix + "HiveTestJob"

  val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val queryConfig = ConfigFactory.parseString(
    """sql = "SELECT firstName, lastName FROM `default`.`test_addresses` WHERE city = 'San Jose'" """
  )

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
      HiveJobSpec.getContextConfig(false, HiveJobSpec.contextConfig) ))
  }

  describe("Spark Hive Jobs") {
    it("should be able to create a Hive table, then query it using separate Hive-SQL jobs") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(30 seconds, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", hiveLoaderClass, emptyConfig, syncEvents ++ errorEvents)
      expectMsgPF(120 seconds, "Did not get JobResult") {
        case JobResult(_, result: Long) => result should equal (3L)
      }
      expectNoMsg()

      manager ! JobManagerActor.StartJob("demo", hiveQueryClass, queryConfig, syncEvents ++ errorEvents)
      expectMsgPF(6 seconds, "Did not get JobResult") {
        case JobResult(_, result: Array[Row]) =>
          result should have length 2
          result(0)(0) should equal ("Bob")
      }
      expectNoMsg()
    }
  }
}
