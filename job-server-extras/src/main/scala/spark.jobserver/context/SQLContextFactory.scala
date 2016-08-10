package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalactic.{Bad, Good, Or}
import org.slf4j.LoggerFactory
import spark.jobserver._
import spark.jobserver.api.JSparkJob

class SQLContextFactory extends ScalaContextFactory {
  type C = SQLContext with ContextLike

  def isValidJob(job: api.SparkJobBase): Boolean = job.isInstanceOf[SparkSqlJob]

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    new SQLContext(new SparkContext(sparkConf)) with ContextLike {
      def stop() { this.sparkContext.stop() }
    }
  }
}

class JavaSqlContextFactory extends SparkContextFactory {
  type C = SQLContext with ContextLike
  type J = JavaJobContainer
  type JC = JSparkSqlJob[_]

  private val logger = LoggerFactory.getLogger(getClass)

  def isValidJob(job: JSparkJob[_, _]): Boolean = job.isInstanceOf[JSparkJob[_, _]]

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    new SQLContext(new SparkContext(sparkConf)) with ContextLike {
      def stop() { this.sparkContext.stop() }
    }
  }

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
}