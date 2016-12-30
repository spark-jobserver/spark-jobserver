package spark.jobserver.python

import java.io.File

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalactic.{Bad, Good, Or}
import org.slf4j.LoggerFactory
import spark.jobserver._
import spark.jobserver.context.{JobLoadError, LoadingError, SparkContextFactory}
import spark.jobserver.util.SparkJobUtils

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait PythonContextLike extends ContextLike {

  /**
    * The Python Subprocess needs to know what sort of context to build from the JVM context.
    * It can't interrogate the JVM type system, so this method is used as an explicit indicator.
    *
    * @return the full canonical class name of the context type
    */
  def contextType: String

  /**
    *
    * @return The entries with which to populate the PYTHONPATH environment variable when
    *         launching the python subprocess.
    */
  def pythonPath: Seq[String]

  /**
    * Which process to call to execute the Python interpreter, e.g `python`, `python3`
    *
    * @return the executable to call
    */
  def pythonExecutable: String

  /**
    * Any mutable actions which need to be taken before the context is used.
    */
  def setupTasks(): Unit
}

trait PythonContextFactory extends SparkContextFactory {

  type J = PythonJobContainer[C]

  override type C <: PythonContextLike

  /**
    * Loads the job of the given appName, version, and class path, and validates that it is
    * the right type of job given the current context type.  For example, it may load a JAR
    * and validate the classpath exists and try to invoke its constructor.
    */
  override def loadAndValidateJob(appName: String,
                                  uploadTime: DateTime,
                                  classPath: String,
                                  jobCache: JobCache): J Or LoadingError = {
    Try(jobCache.getPythonJob(appName, uploadTime, classPath)) match {
      case Success(jobInfo) => Good(PythonJobContainer(buildJob(jobInfo.eggPath, classPath)))
      case Failure(ex: Exception) => Bad(JobLoadError(ex))
      case Failure(ex) => Bad(JobLoadError(new Exception(ex)))
    }
  }

  def buildJob(eggPath: String, modulePath:String): PythonJob[C] =
    PythonJob[C](eggPath, modulePath, py4JImports)

  /**
    *
    * @return List of classes which will be imported through the gateway by the Python process,
    *         using the form `java_import(gateway.jvm, "org.apache.spark.SparkConf")`
    */
  def py4JImports: Seq[String]

  /**
    * Partial implementation of makeContext to avoid repetition in the ContextFactory implementations.
    * Does the generic setup tasks and delegates to doMakeContext
    * @param sparkConf the Spark Context configuration.
    * @param contextConfig
    * @param contextName the name of the context to start
    * @return the newly created context.
    */
  override def makeContext(sparkConf: SparkConf,
                           contextConfig: Config,
                           contextName: String): C = {
    val sc = new SparkContext(sparkConf.set("spark.yarn.isPython", "true"))
    val specificSc = doMakeContext(sc, contextConfig, contextName)
    specificSc.setupTasks()
    specificSc
  }

  protected def doMakeContext(sc: SparkContext,
                    contextConfig: Config,
                    contextName: String): C
}

object PythonContextFactory {
  val sparkContextImports =
    Seq(
      "org.apache.spark.SparkConf",
      "org.apache.spark.api.java.*",
      "org.apache.spark.api.python.*",
      "scala.Tuple2",
      "org.apache.spark.mllib.api.python.*",
      "org.apache.spark.sql.SQLContext",
      "org.apache.spark.sql.UDFRegistration",
      "org.apache.spark.sql.hive.HiveContext"
    )

  val sqlContextImports = sparkContextImports ++ Seq(
    "org.apache.spark.sql.*"
  )

  val hiveContextImports = sqlContextImports ++ Seq(
    "org.apache.spark.sql.hive.*"
  )
}

trait DefaultContextLikeImplementations {

  self: PythonContextLike =>

  def config: Config

  lazy val logger = LoggerFactory.getLogger(getClass)

  override lazy val pythonPath: Seq[String] = pythonPaths(config)

  protected def pythonPaths(config: Config): Seq[String] = {
    val envPaths = sys.env.get("PYTHONPATH").map(_.split(":").toSeq).getOrElse(Seq())
    val configPaths = config.getStringList("python.paths").asScala
    //Allow relative paths in config:
    val pyPaths = (envPaths ++ configPaths).map(p => new File(p).getAbsolutePath)
    logger.info(s"Python paths for context: ${pyPaths.mkString("[", ", ", "]")}")
    pyPaths
  }

  override lazy val pythonExecutable: String = config.getString("python.executable")

  override def setupTasks(): Unit = {
    for ((k, v) <- SparkJobUtils.getHadoopConfig(config)) sparkContext.hadoopConfiguration.set(k, v)
  }
}

class PythonSparkContextFactory extends PythonContextFactory {

  override type C = JavaSparkContext with PythonContextLike

  override def doMakeContext(sc: SparkContext,
                           contextConfig: Config,
                           contextName: String): JavaSparkContext with PythonContextLike = {
    val jsc = new JavaSparkContext(sc) with PythonContextLike with DefaultContextLikeImplementations {
      override val config = contextConfig
      override val sparkContext: SparkContext = sc
      override val contextType = classOf[JavaSparkContext].getCanonicalName
    }
    jsc
  }

  override def py4JImports: Seq[String] = PythonContextFactory.sparkContextImports
}

class PythonSQLContextFactory extends PythonContextFactory {

  override type C = SQLContext with PythonContextLike

  override def py4JImports: Seq[String] =
    PythonContextFactory.sqlContextImports

  override def doMakeContext(sc: SparkContext,
                           contextConfig: Config,
                           contextName: String): SQLContext with PythonContextLike = {
    val jSqlContext = new SQLContext(sc) with PythonContextLike with DefaultContextLikeImplementations {
      override val config = contextConfig
      override val contextType: String = classOf[SQLContext].getCanonicalName
      override def stop(): Unit = sc.stop()
    }
    jSqlContext
  }
}

class PythonHiveContextFactory extends PythonContextFactory {

  override type C = HiveContext with PythonContextLike

  override def py4JImports: Seq[String] =
    PythonContextFactory.hiveContextImports

  override def doMakeContext(sc: SparkContext,
                           contextConfig: Config,
                           contextName: String): HiveContext with PythonContextLike = {
    val jHiveContext = new HiveContext(sc) with PythonContextLike with DefaultContextLikeImplementations {
      override val contextType: String = classOf[HiveContext].getCanonicalName
      override def config: Config = contextConfig
      override def stop(): Unit = sc.stop()
    }
    jHiveContext
  }
}
