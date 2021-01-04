package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.context.{SessionContextFactory, SparkSessionContextLikeWrapper}
import spark.jobserver.io.{InMemoryBinaryDAO, InMemoryMetaDAO, JobDAOActor}
import spark.jobserver.util.{JobserverConfig, SparkJobUtils}

import scala.concurrent.duration._

class TestSessionContextFactory extends SessionContextFactory {

  override def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val builder = SparkSession.builder()
    builder.config(sparkConf).appName(contextName).master("local")
    builder.config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:myDB;create=true")
    builder.config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    super.setupHiveSupport(config, builder)
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
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryDAO
    daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
    // JobManagerTestActor is used to take advantage of CleanlyStoppingSparkContextJobManagerActor class
    manager = system.actorOf(JobManagerTestActor.props(daoActor))
  }

  after {
    JobManagerTestActor.stopSparkContextIfAlive(system, manager) should be(true)
    AkkaTestUtils.shutdownAndWait(manager)
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
      expectNoMessage(1.seconds)

      manager ! JobManagerActor.StartJob(
        hiveQueryClass, Seq(testBinInfo), queryConfig, syncEvents ++ errorEvents)
      expectMsgPF(6 seconds, "Did not get JobResult") {
        case JobResult(_, result: Array[Row]) =>
          result should have length 2
          result(0)(0) should equal ("Bob")
      }
      expectNoMessage(1.seconds)
    }

    /**
      * The nature of exception can vary e.g. if you are trying to use Hive
      * with Spark 2.4.4 and Hadoop 3.2.0, then you will get
      * "Unrecognized Hadoop major version number: 3.2.0" since Hive is not fully
      * compatible but if you are using Spark/Hadoop which is compatible with Hive
      * then you will get
      * "org.apache.spark.sql.AnalysisException: Hive support is required to CREATE Hive TABLE (AS SELECT);;"
      */
    it("should throw exception if hive is disabled") {
      val configWithHiveDisabled = ConfigFactory.parseString(
        s"${JobserverConfig.IS_SPARK_SESSION_HIVE_ENABLED}=false").withFallback(contextConfig)

      val exception = intercept[java.lang.AssertionError] {
        manager ! JobManagerActor.Initialize(configWithHiveDisabled, None, emptyActor)
        expectMsgClass(30 seconds, classOf[JobManagerActor.Initialized])

        val testBinInfo = uploadTestJar()
        manager ! JobManagerActor.StartJob(
          hiveLoaderClass, Seq(testBinInfo), emptyConfig, syncEvents ++ errorEvents)
        expectMsgPF(120 seconds, "Did not get JobResult") {
          case JobResult(_, result: Long) => result should equal (3L)
        }
      }

      exception.getMessage.contains(
        "org.apache.spark.sql.AnalysisException: Hive support is required") should be(true)
    }
  }
}
