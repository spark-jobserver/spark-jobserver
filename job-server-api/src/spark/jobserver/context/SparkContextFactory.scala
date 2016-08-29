package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalactic._
import org.slf4j.LoggerFactory
import spark.jobserver.api
import spark.jobserver.util.SparkJobUtils
import spark.jobserver.{ContextLike, JobCache, SparkJob}

trait JobContainer {
  def getSparkJob: api.SparkJobBase
}

sealed trait LoadingError
case object JobClassNotFound extends LoadingError
case object JobWrongType extends LoadingError
case class JobLoadError(ex: Exception) extends LoadingError

/**
 * Factory trait for creating a SparkContext or any derived Contexts,
 * such as SQLContext, StreamingContext, HiveContext, etc.
 * My implementing classes can be dynamically loaded using classloaders to ensure that the entire
 * SparkContext has access to certain dynamically loaded classes, for example, job jars.
 * Also, this is capable of loading jobs which don't necessarily implement SparkJobBase, or even
 * Python jobs etc., so long as a wrapping SparkJobBase is returned.
 */
trait SparkContextFactory {
  import SparkJobUtils._

  type C <: ContextLike
  type J <: JobContainer

  /**
   * Loads the job of the given appName, version, and class path, and validates that it is
   * the right type of job given the current context type.  For example, it may load a JAR
   * and validate the classpath exists and try to invoke its constructor.
   */
  def loadAndValidateJob(appName: String,
                         uploadTime: DateTime,
                         classPath: String,
                         jobCache: JobCache): J Or LoadingError

  /**
   * Creates a SparkContext or derived context.
   * @param sparkConf the Spark Context configuration.
   * @param config the context config
   * @param contextName the name of the context to start
   * @return the newly created context.
   */
  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C

  /**
   * Creates a SparkContext or derived context.
   * @param config the overall system / job server Typesafe Config
   * @param contextConfig the config specific to this particular context
   * @param contextName the name of the context to start
   * @return the newly created context.
   */
  def makeContext(config: Config, contextConfig: Config, contextName: String): C = {
    val sparkConf = configToSparkConf(config, contextConfig, contextName)
    makeContext(sparkConf, contextConfig, contextName)
  }
}

case class ScalaJobContainer(job: api.SparkJobBase) extends JobContainer {
  def getSparkJob: api.SparkJobBase = job
}

/**
 * A SparkContextFactory designed for Scala and Java jobs that loads jars
 */
trait ScalaContextFactory extends SparkContextFactory {
  type J = ScalaJobContainer

  val logger = LoggerFactory.getLogger(getClass)

  def loadAndValidateJob(appName: String,
                         uploadTime: DateTime,
                         classPath: String,
                         jobCache: JobCache): J Or LoadingError = {
    logger.info("Loading class {} for app {}", classPath, appName: Any)
    val jobJarInfo = try {
      jobCache.getSparkJob(appName, uploadTime, classPath)
    } catch {
      case _: ClassNotFoundException => return Bad(JobClassNotFound)
      case err: Exception            => return Bad(JobLoadError(err))
    }

    // Validate that job fits the type of context we launched
    val job = jobJarInfo.constructor()
    if (isValidJob(job)) { Good(ScalaJobContainer(job)) }
    else                 { Bad(JobWrongType) }
  }

  /**
   * Returns true if the job is valid for this context.
   * At the minimum this should check for if the job can actually take a context of this type;
   * for example, a SQLContext should only accept jobs that take a SQLContext.
   * The recommendation is to define a trait for each type of context job;  the standard
   * [[DefaultSparkContextFactory]] checks to see if the job is of type [[SparkJob]].
   */
  def isValidJob(job: api.SparkJobBase): Boolean
}

/**
 * The default factory creates a standard SparkContext.
 * In the future if we want to add additional methods, etc. then we can have additional factories.
 * For example a specialized SparkContext to manage RDDs in a user-defined way.
 *
 * If you create your own SparkContextFactory, please make sure it has zero constructor args.
 */
class DefaultSparkContextFactory extends ScalaContextFactory {

  type C = SparkContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    val sc = new SparkContext(sparkConf) with ContextLike {
      def sparkContext: SparkContext = this
    }
    for ((k, v) <- SparkJobUtils.getHadoopConfig(config)) sc.hadoopConfiguration.set(k, v)
    sc
  }

  def isValidJob(job: api.SparkJobBase): Boolean =
    job.isInstanceOf[SparkJob] || job.isInstanceOf[api.SparkJob]
}
