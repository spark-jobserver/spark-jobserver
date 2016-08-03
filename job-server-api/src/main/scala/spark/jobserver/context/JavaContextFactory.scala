package spark.jobserver.context
import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalactic.{Bad, Good, Or}
import org.slf4j.LoggerFactory
import spark.jobserver.api.JSparkJob
import spark.jobserver.util.SparkJobUtils
import spark.jobserver.{ContextLike, JobCache}


class JavaContextFactory extends SparkContextFactory {
  type C = JavaSparkContext with ContextLike
  type J = JavaJobContainer
  type JC = JSparkJob[_]

  private val logger = LoggerFactory.getLogger(getClass)
  def loadAndValidateJob(appName: String,
                                  uploadTime: DateTime,
                                  classPath: String,
                                  jobCache: JobCache): J Or LoadingError = {

    logger.info("Loading class {} for app {}", classPath, appName: Any)
    val jobJarInfo = try {
      jobCache.getJavaJob(appName, uploadTime, classPath)
    } catch {
      case _: ClassNotFoundException => return Bad(JobClassNotFound)
      case err: Exception            => return Bad(JobLoadError(err))
    }
    val job = jobJarInfo.constructor()
    if (isValidJob(job)) {
      Good(JavaJobContainer(job))
    } else {
      Bad(JobWrongType)
    }
  }
  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val sc = new JavaSparkContext(SparkContext.getOrCreate(sparkConf)) with ContextLike {
      def sparkContext: SparkContext = this.sc
    }
    for ((k, v) <- SparkJobUtils.getHadoopConfig(config)) sc.hadoopConfiguration.set(k, v)
    sc
  }
  def isValidJob(job: JC): Boolean = job.isInstanceOf[JC]
}

case class JavaJobContainer(job: JSparkJob[_]) extends JobContainer[JSparkJob[_]] {
  override def getSparkJob: JSparkJob[_] = job
}