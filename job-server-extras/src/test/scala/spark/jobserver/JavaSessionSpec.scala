package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import spark.jobserver.context.{JavaContextFactory, SparkSessionContextLikeWrapper}
import spark.jobserver.japi.{BaseJavaJob, JSessionJob}
import spark.jobserver.util.SparkJobUtils
import org.apache.spark.sql.Row
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.io.JobDAOActor

import scala.concurrent.duration._


class JavaTestSessionContextFactory extends JavaContextFactory {
  type C = SparkSessionContextLikeWrapper

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JSessionJob[_]]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
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

object JavaSessionSpec extends JobSpecConfig {
  override val contextFactory = classOf[JavaTestSessionContextFactory].getName
}

class JavaSessionSpec extends ExtrasJobSpecBase(JavaSessionSpec.getNewSystem) {

  val classPrefix = "spark.jobserver."
  private val hiveLoaderClass = classPrefix + "JSessionTestLoaderJob"
  private val hiveQueryClass = classPrefix + "JSessionTestJob"

  val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val queryConfig = ConfigFactory.parseString(
    """sql = "SELECT firstName, lastName FROM `default`.`test_addresses` WHERE city = 'San Jose'" """
  )

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
      JavaSessionSpec.getContextConfig(false, JavaSessionSpec.contextConfig),
      daoActor))
  }

  describe("Java Session Jobs") {
    it("should be able to create a Hive table, then query it using separate Spark-SQL jobs") {
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
