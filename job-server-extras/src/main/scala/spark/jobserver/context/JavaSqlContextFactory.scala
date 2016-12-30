package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalactic.{Bad, Good, Or}
import spark.jobserver.japi.JavaJob
import spark.jobserver.{ContextLike, JHiveJob, JSqlJob, JobCache}

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
    val sc = SparkContext.getOrCreate(sparkConf)
    val hiveCtx = new HiveContext(sc) with ContextLike {
      override def stop() = this.stop()
    }
    hiveCtx
  }
}

/*
class JavaStreamingContextFactory extends SparkContextFactory {
  type C = JavaStreamingContext with ContextLike
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
    if (j.job.isInstanceOf[JSparkJob[_]]) {
      Good(ScalaJobContainer(JavaJob(j.job)))
    } else {
      Bad(JobWrongType)
    }
  }

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")
    val stopGracefully = config.getBoolean("streaming.stopGracefully")
    val stopSparkContext = config.getBoolean("streaming.stopSparkContext")
    val jCtxt = new JavaStreamingContext(sparkConf, Milliseconds(interval)) with ContextLike
    jCtxt
  }
}
*/