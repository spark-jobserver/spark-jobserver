package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.context.{SessionContextFactory, SparkSessionContextLikeWrapper}
import spark.jobserver.io.JobDAOActor
import spark.jobserver.util.SparkJobUtils

import scala.concurrent.duration._

class TestSessionContextFactory extends SessionContextFactory {

  override def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    val builder = SparkSession.builder()
    builder.config(sparkConf).appName(contextName).master("local")
    builder.config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:myDB;create=true")
    builder.config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    try {
      builder.enableHiveSupport()
    } catch {
      case e: IllegalArgumentException => logger.warn(s"Hive support not enabled - ${e.getMessage()}")
    }
    val spark = builder.getOrCreate()
    for ((k, v) <- SparkJobUtils.getHadoopConfig(config)) spark.sparkContext.hadoopConfiguration.set(k, v)
    SparkSessionContextLikeWrapper(spark)
  }
}

object SessionJobSpec extends JobSpecConfig {
  override val contextFactory = classOf[TestSessionContextFactory].getName
}

class SessionJobSpec extends ExtrasJobSpecBase(SessionJobSpec.getNewSystem) {

  val classPrefix = "spark.jobserver."
  private val hiveLoaderClass = classPrefix + "SessionLoaderJob"
  private val hiveQueryClass = classPrefix + "SessionTestJob"

  val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val queryConfig = ConfigFactory.parseString(
    """sql = "SELECT firstName, lastName FROM `default`.`test_addresses` WHERE city = 'San Jose'" """
  )

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
      SessionJobSpec.getContextConfig(false, SessionJobSpec.contextConfig)))
  }

  describe("Spark Session Jobs") {
    it("should be able to create a Hive table, then query it using separate Spark-SQL jobs") {
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
