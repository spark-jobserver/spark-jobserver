package spark.jobserver.util

import com.typesafe.config.Config
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.LoggerFactory

import scala.util.Try
/**
 * This class aims to eliminate the need to call spark-submit
 * through scripts.
 *
 * When server_start.sh is executed it sources the setenv.sh
 * script. set -a flag enables exporting the variables to
 * environment. Launcher uses those environment variables to
 * start context JVMs using SparkLauncher class.
 */
abstract class Launcher(config: Config, sparkLauncher: SparkLauncher, enviornment: Environment) {
    private val logger = LoggerFactory.getLogger("spark-launcher")
    private var handler: SparkAppHandle = null

    protected final val master = config.getString("spark.master")
    protected final val defaultSuperviseModeEnabled = Try(
        config.getBoolean(ManagerLauncher.SJS_SUPERVISE_MODE_KEY)).getOrElse(false)
    protected final val deployMode = config.getString("spark.submit.deployMode")
    protected final val sjsJarPath = getEnvironmentVariable("MANAGER_JAR_FILE")
    protected final val baseGCOPTS = getEnvironmentVariable("GC_OPTS_BASE")
    protected final val baseJavaOPTS = getEnvironmentVariable("JAVA_OPTS_BASE")
    protected val launcher = sparkLauncher

    protected def addCustomArguments()

    final def start(): (Boolean, String) = {
      validate() match {
        case (false, error) => return (false, error)
        case _ =>
      }

      initSparkLauncher()

      try {
        logger.info("Adding custom arguments to launcher")
        addCustomArguments()

        logger.info("Start launcher application")
        handler = launcher.startApplication()
        (true, "")
      } catch {
        case err: Exception =>
          logger.error("Failed to launch", err);
          (false, err.getMessage)
      }
    }

    protected def getEnvironmentVariable(name: String, default: String = ""): String = {
      enviornment.get(name, default)
    }

    private def initSparkLauncher() {
      logger.info("Initializing spark launcher")
      launcher.setSparkHome(getEnvironmentVariable("SPARK_HOME"))
      launcher.setDeployMode(deployMode)
      launcher.setAppResource(sjsJarPath)
      launcher.setVerbose((getEnvironmentVariable("SPARK_LAUNCHER_VERBOSE") == "1"))
      launcher.setConf("spark.master.rest.enabled", "true")
    }

    protected def validate(): (Boolean, String) = {
      new HadoopFSFacade(defaultFS = "file:///").isFile(sjsJarPath) match {
        case Some(true) => (true, "")
        case Some(false) => (false, s"job-server jar file doesn't exist at $sjsJarPath")
        case None => (false, "Unexpected error occurred while reading file. Check logs")
      }
    }
}
