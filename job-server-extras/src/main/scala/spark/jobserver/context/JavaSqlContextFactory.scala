package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalactic._
import spark.jobserver.japi._
import spark.jobserver.{ContextLike, JobCache}

class JavaSqlContextFactory extends SparkContextFactory {
  type C = SQLContext with ContextLike
  type J = ScalaJobContainer

  def loadAndValidateJob(appName: String,
                         uploadTime: DateTime,
                         classPath: String,
                         jobCache: JobCache): J Or LoadingError = {

    val j = try {
      jobCache.getJavaJob(appName, uploadTime, classPath)
    } catch {
      case _: ClassNotFoundException => return Bad(JobClassNotFound)
      case err: Exception => return Bad(JobLoadError(err))
    }
    if (j.job.isInstanceOf[JSqlJob[_]]) {
      Good(ScalaJobContainer(JavaJob(j.job)))
    } else {
      Bad(JobWrongType)
    }
  }

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val sc = new SparkContext(sparkConf)
    new SQLContext(sc) with ContextLike {
      def stop(): Unit = this.sparkContext.stop()
    }
  }
}

class JavaHiveContextFactory extends SparkContextFactory {
  type C = HiveContext with ContextLike
  type J = ScalaJobContainer

  def loadAndValidateJob(appName: String,
                         uploadTime: DateTime,
                         classPath: String,
                         jobCache: JobCache): J Or LoadingError = {

    val j = try {
      jobCache.getJavaJob(appName, uploadTime, classPath)
    } catch {
      case _: ClassNotFoundException => return Bad(JobClassNotFound)
      case err: Exception => return Bad(JobLoadError(err))
    }
    if (j.job.isInstanceOf[JHiveJob[_]]) {
      Good(ScalaJobContainer(JavaJob(j.job)))
    } else {
      Bad(JobWrongType)
    }
  }

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    contextFactory(sparkConf)
  }

  protected def contextFactory(conf: SparkConf): C = {
    new HiveContext(new SparkContext(conf)) with HiveContextLike
  }
}


class JavaStreamingContextFactory extends SparkContextFactory {
  type C = StreamingContext with ContextLike
  type J = ScalaJobContainer

  def loadAndValidateJob(appName: String,
                         uploadTime: DateTime,
                         classPath: String,
                         jobCache: JobCache): J Or LoadingError = {

    val j = try {
      jobCache.getJavaJob(appName, uploadTime, classPath)
    } catch {
      case _: ClassNotFoundException => return Bad(JobClassNotFound)
      case err: Exception => return Bad(JobLoadError(err))
    }
    if (j.job.isInstanceOf[JStreamingJob[_]]) {
      Good(ScalaJobContainer(JavaJob(j.job)))
    } else {
      Bad(JobWrongType)
    }
  }

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
