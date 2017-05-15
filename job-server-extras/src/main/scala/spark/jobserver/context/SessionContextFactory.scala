package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import spark.jobserver.{ContextLike, SparkSessionJob}
import spark.jobserver.api.SparkJobBase
import spark.jobserver.util.SparkJobUtils

case class SparkSessionContextLikeWrapper(spark: SparkSession) extends ContextLike {
  def sparkContext: SparkContext = spark.sparkContext
  def stop() {
    spark.close()
  }
}

class SessionContextFactory extends ScalaContextFactory {
  type C = SparkSessionContextLikeWrapper

  def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SparkSessionJob]

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    val spark = SparkSession.builder.config(sparkConf).appName(contextName).enableHiveSupport().getOrCreate()
    for ((k, v) <- SparkJobUtils.getHadoopConfig(config)) spark.sparkContext.hadoopConfiguration.set(k, v)
    SparkSessionContextLikeWrapper(spark)
  }
}
