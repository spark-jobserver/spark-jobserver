package spark.jobserver.context

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import spark.jobserver.{ContextLike, SparkStreamingJob, api}
import spark.jobserver.util.JobserverConfig

class StreamingContextFactory extends ScalaContextFactory {

  type C = StreamingContext with ContextLike

  def isValidJob(job: api.SparkJobBase): Boolean = job.isInstanceOf[SparkStreamingJob]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")
    val stopGracefully = config.getBoolean("streaming.stopGracefully")
    val stopSparkContext = config.getBoolean("streaming.stopSparkContext")
    new StreamingContext(sparkConf, Milliseconds(interval)) with ContextLike {
      def stop() {
        //Gracefully stops the spark context
        stop(stopSparkContext, stopGracefully)
      }
    }
  }

  override def updateConfig(contextConfig: Config): Config = {
    contextConfig.hasPath(JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR) match {
      case true => logger.info(
        s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR} already configured, not changing the config")
        contextConfig
      case false =>
        logger.warn(
          s"""${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR} is not set. Streaming contexts have
          |default set to true. On any error the context will stop. To change behavior,
          |set ${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR} while creating context."""
          .stripMargin.replaceAll("\n", " "))

        contextConfig.withFallback(
          ConfigFactory.parseString(s"${JobserverConfig.STOP_CONTEXT_ON_JOB_ERROR}=true"))
    }
  }
}
