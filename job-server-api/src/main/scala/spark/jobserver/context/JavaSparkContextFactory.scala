package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalactic._
import spark.jobserver.japi.{JSparkJob, JavaJob}
import spark.jobserver.{ContextLike, JobCache}

class JavaSparkContextFactory extends SparkContextFactory {
  type C = JavaSparkContext with ContextLike
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
    val jsc = new JavaSparkContext(sparkConf) with ContextLike {
      override def sparkContext: SparkContext = this.sc
    }
    jsc
  }

}
