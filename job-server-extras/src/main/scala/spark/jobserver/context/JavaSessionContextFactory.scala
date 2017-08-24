package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.SparkConf
import spark.jobserver.ContextLike
import spark.jobserver.japi.{BaseJavaJob, JSessionJob, JStreamingJob}
import spark.jobserver.util.SparkJobUtils

class JavaSessionContextFactory extends JavaContextFactory {
  type C = SparkSessionContextLikeWrapper

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JSessionJob[_]]

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

class JavaStreamingContextFactory extends JavaContextFactory {
  type C = StreamingContext with ContextLike

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JStreamingJob[_]]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")
    val stopGracefully = config.getBoolean("streaming.stopGracefully")
    val stopSparkContext = config.getBoolean("streaming.stopSparkContext")
    new StreamingContext(sparkConf, Milliseconds(interval)) with ContextLike {
      def stop() {
        stop(stopSparkContext, stopGracefully)
      }
    }
  }
}
