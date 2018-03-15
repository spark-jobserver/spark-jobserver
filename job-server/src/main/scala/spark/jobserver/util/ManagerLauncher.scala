package spark.jobserver.util

import java.io.File
import scala.util.Try
import collection.JavaConverters._
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.apache.spark.launcher.SparkLauncher

class ManagerLauncher(systemConfig: Config, contextConfig: Config,
                      masterAddress: String, contextActorName: String, contextDir: String,
                      sparkLauncher: SparkLauncher = new SparkLauncher,
                      environment: Environment = new SystemEnvironment)
                      extends Launcher(systemConfig, sparkLauncher, environment) {
  val logger = LoggerFactory.getLogger("spark-context-launcher")

  var loggingOpts = getEnvironmentVariable("MANAGER_LOGGING_OPTS")
  val configOverloads = getEnvironmentVariable("CONFIG_OVERRIDES")
  val extraSparkConfigurations = getEnvironmentVariable("MANAGER_EXTRA_SPARK_CONFS")
  val extraJavaOPTS = getEnvironmentVariable("MANAGER_EXTRA_JAVA_OPTIONS")
  var gcOPTS = baseGCOPTS

  override def addCustomArguments() {
      if (deployMode == "client") {
        val gcFilePath = new File(contextDir,
            getEnvironmentVariable("GC_OUT_FILE_NAME", "gc.out")).toString()
        loggingOpts += s" -DLOG_DIR=$contextDir"
        gcOPTS += s" -Xloggc:$gcFilePath"
      }

      val contextSparkMaster = Try(contextConfig.getString("launcher.spark.master"))
        .getOrElse(SparkMasterProvider.fromConfig(systemConfig).getSparkMaster(systemConfig))
      launcher.setMaster(contextSparkMaster)
      launcher.setMainClass("spark.jobserver.JobManager")
      launcher.addAppArgs(masterAddress, contextActorName, getEnvironmentVariable("MANAGER_CONF_FILE"))
      launcher.addSparkArg("--conf", s"spark.executor.extraJavaOptions=$loggingOpts")
      launcher.addSparkArg("--driver-java-options",
          s"$gcOPTS $baseJavaOPTS $loggingOpts $configOverloads $extraJavaOPTS")

      if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
         launcher.addSparkArg("--proxy-user", contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
      }

      for (e <- Try(contextConfig.getConfig("launcher"))) {
         e.entrySet().asScala.map { c =>
            launcher.addSparkArg("--conf", s"${c.getKey}=${c.getValue.unwrapped.toString}")
         }
      }

      parseExtraSparkConfigurations() match {
        case Some(configs) =>
          configs.filter(conf => conf.contains("=")).foreach(conf => launcher.addSparkArg("--conf", conf))
        case None =>
      }

      val submitOutputFile = new File(contextDir, "spark-job-server.out")
      // This function was introduced in 2.1.0 Spark version, for older versions, it will
      // break with MethodNotFound exception.
      launcher.redirectOutput(submitOutputFile)
      launcher.redirectError(submitOutputFile)
    }

  override def validate(): (Boolean, String) = {
     super.validate() match {
       case (true, _) =>
         validateMemory(contextConfig.getString("launcher.spark.driver.memory")) match {
           case true => (true, "")
           case false => (false,
             "Context error: spark.driver.memory has invalid value. Accepted formats 1024k, 2g, 512m")
         }
       case (false, error) => (false, error)
     }
  }

  private def parseExtraSparkConfigurations(): Option[Array[String]] = {
    if (!extraSparkConfigurations.isEmpty()) {
      return Some(extraSparkConfigurations.trim().split('|'))
    }
    None
  }

  private def validateMemory(providedMemory: String): Boolean = {
    import scala.util.matching.Regex
    val pattern = "^[0-9]+[kmgt]?$".r

    pattern.findFirstIn(providedMemory).isDefined
  }
}
