package spark.jobserver.context

import com.typesafe.config.Config
import net.spy.memcached.compat.log.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.joda.time.DateTime
import org.scalactic.{Bad, Good, Or}
import spark.jobserver._
import spark.jobserver.api.JSparkJob

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
}

class JavaStreamingContextFactory extends SparkContextFactory {
  type C = StreamingContext with ContextLike
  type J = JavaJobContainer
  type JC = JSparkStreamingJob[_]

  private val logger = LoggerFactory.getLogger(getClass)

  private def isValidJob(job: JSparkJob[_, _]) = job.isInstanceOf[JSparkJob[_, _]]

  def loadAndValidateJob(appName: String,
                         uploadTime: DateTime,
                         classPath: String,
                         jobCache: JobCache): J Or LoadingError = {

    val jobJarInfo = try {
      jobCache.getJavaJob(appName, uploadTime, classPath)
    } catch {
      case _: ClassNotFoundException => return Bad(JobClassNotFound)
      case err: Exception => return Bad(JobLoadError(err))
    }
    val job = jobJarInfo.constructor()
    if (isValidJob(job)) {
      Good(JavaJobContainer(job))
    } else {
      Bad(JobWrongType)
    }
  }

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")
    val stopGracefully = config.getBoolean("streaming.stopGracefully")
    val stopSparkContext = config.getBoolean("streaming.stopSparkContext")
    new StreamingContext(SparkContext.getOrCreate(sparkConf), Milliseconds(interval)) with ContextLike {
      override def stop() {
        stop(stopSparkContext, stopGracefully)
      }
    }
  }
}

