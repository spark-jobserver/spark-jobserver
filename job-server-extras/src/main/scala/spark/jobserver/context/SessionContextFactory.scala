package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import spark.jobserver.{ContextLike, SparkSessionJob}
import spark.jobserver.api.SparkJobBase
import spark.jobserver.util.SparkJobUtils

case class SparkSessionContextLikeWrapper(spark: SparkSession) extends ContextLike {
  def sparkContext: SparkContext = spark.sparkContext
  def stop() {
    spark.stop()
  }
}

class SessionContextFactory extends ScalaContextFactory {
  type C = SparkSessionContextLikeWrapper

  def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SparkSessionJob]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val builder = SparkSession.builder()
    builder.config(sparkConf).appName(contextName)
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