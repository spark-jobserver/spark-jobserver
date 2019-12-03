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

  override def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
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
  private val hiveLoaderClass = classPrefix + "SessionLoaderTestJob"
  private val hiveQueryClass = classPrefix + "SessionTestJob"

  val emptyConfig = ConfigFactory.parseString("""
      |spark.master = bar
      |cp = ["demo"]
    """.stripMargin
  )
  val queryConfig = ConfigFactory.parseString(
    """
      |sql = "SELECT firstName, lastName FROM `test_addresses` WHERE city = 'San Jose'"
      |cp = ["demo"]
      |""".stripMargin
  )
  lazy val contextConfig = SessionJobSpec.getContextConfig(false, SessionJobSpec.contextConfig)

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(daoActor))
  }

  describe("Spark Session Jobs") {
    it("should be able to create a Hive table, then query it using separate Spark-SQL jobs") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(30 seconds, classOf[JobManagerActor.Initialized])

      var testBinInfo = uploadTestJar()
      manager ! JobManagerActor.StartJob(
        hiveLoaderClass, Seq(testBinInfo), emptyConfig, syncEvents ++ errorEvents)
      expectMsgPF(120 seconds, "Did not get JobResult") {
        case JobResult(_, result: Long) => result should equal (3L)
      }
      expectNoMsg(1.seconds)

      manager ! JobManagerActor.StartJob(
        hiveQueryClass, Seq(testBinInfo), queryConfig, syncEvents ++ errorEvents)
      expectMsgPF(6 seconds, "Did not get JobResult") {
        case JobResult(_, result: Array[Row]) =>
          result should have length 2
          result(0)(0) should equal ("Bob")
      }
      expectNoMsg(1.seconds)
    }
  }
}
