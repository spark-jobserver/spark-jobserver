package spark.jobserver.util

import scala.util.Try
import scala.sys.process.{Process, ProcessLogger}
import java.io.File
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.launcher.SparkAppHandle

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
    }

    protected def validate(): (Boolean, String) = {
      if (!new File(sjsJarPath).isFile()) {
        val errorMsg =
          s"Environment error: job-server jar file doesn't exist. Path is $sjsJarPath"
        logger.error(errorMsg)
        return (false, errorMsg)
      }
      return (true, "")
    }
}
