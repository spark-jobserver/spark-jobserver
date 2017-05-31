package spark.jobserver

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.context.{HiveContextFactory, HiveContextLike}
import spark.jobserver.io.JobDAOActor

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class TestHiveContextFactory extends HiveContextFactory {
  override protected def contextFactory(conf: SparkConf): C = {
    val sc = SparkContext.getOrCreate(conf)
    Try(new TestHiveContext(sc) with HiveContextLike) match {
      case Success(hc) => hc
      case Failure(e) =>
        sc.stop
        throw e
    }
  }
}

object HiveJobSpec extends JobSpecConfig {
  override val contextFactory = classOf[TestHiveContextFactory].getName
}

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
      HiveJobSpec.getContextConfig(false, HiveJobSpec.contextConfig),
      daoActor))
  }

  describe("Spark Hive Jobs") {
    it("should be able to create a Hive table, then query it using separate Hive-SQL jobs") {
      manager ! JobManagerActor.Initialize(None)
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
