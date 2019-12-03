package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import spark.jobserver.{ContextLike, SparkSessionJob}
import spark.jobserver.api.SparkJobBase
import spark.jobserver.util.{JobserverConfig, SparkJobUtils}
import org.slf4j.LoggerFactory

case class SparkSessionContextLikeWrapper(spark: SparkSession) extends ContextLike {
  val logger = LoggerFactory.getLogger(getClass)
  def sparkContext: SparkContext = spark.sparkContext
  def stop() {
    spark.streams.active.foreach(f => {
            logger.info("Killing stream " + f.name )
            f.stop()
          })
    spark.stop()
  }
}

class SessionContextFactory extends ScalaContextFactory {
  type C = SparkSessionContextLikeWrapper

  def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SparkSessionJob]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val builder = SparkSession.builder()
    builder.config(sparkConf).appName(contextName)
    setupHiveSupport(config, builder)
    val spark = builder.getOrCreate()
    for ((k, v) <- SparkJobUtils.getHadoopConfig(config)) spark.sparkContext.hadoopConfiguration.set(k, v)
    SparkSessionContextLikeWrapper(spark)
  }

  protected def setupHiveSupport(config: Config, builder: SparkSession.Builder) = {
    if (config.getBoolean(JobserverConfig.IS_SPARK_SESSION_HIVE_ENABLED)) {
      try {
        builder.enableHiveSupport()
      } catch {
        case e: IllegalArgumentException => logger.warn(s"Hive support not enabled - ${e.getMessage()}")
      }
    }
  }
}