package spark.jobserver.util

import java.io.File
import java.nio.file.Files

import scala.util.Try
import collection.JavaConverters._
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.slf4j.LoggerFactory
import org.apache.spark.launcher.SparkLauncher
import spark.jobserver.util.ManagerLauncher.getStringSeq

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ManagerLauncher {
  final val SJS_SUPERVISE_MODE_KEY = "spark.driver.supervise"
  final val CONTEXT_SUPERVISE_MODE_KEY = "launcher.spark.driver.supervise"

  def shouldSuperviseModeBeEnabled(sjsSupervisorMode: Boolean,
                                   contextSupervisorMode: Option[Boolean]): Boolean = {
    (sjsSupervisorMode, contextSupervisorMode) match {
      case (_, Some(contextSupervisorMode)) => contextSupervisorMode
      case (_, None) => sjsSupervisorMode
    }
  }

  def getStringSeq(conf: Config, key: String): Seq[String] = {
    Try(conf.getStringList(key).asScala).
      orElse(Try(conf.getString(key).split(",").toSeq)).getOrElse(Nil)
  }

  def apply(config: Config, contextConfig: Config, masterAddress: String,
            contextName: String, contextActorName: String, contextDir: String): ManagerLauncher = {

    // Here we pre-add pyFiles to the PYTHONPATH of the spark-submit process
    val environment: Environment = new SystemEnvironment
    val env = mutable.Map[String, String]()

    val contextSparkMaster = Try(contextConfig.getString("launcher.spark.master"))
      .getOrElse(SparkMasterProvider.fromConfig(config).getSparkMaster(config))
    if (contextConfig.getString("context-factory").contains("python")) {
      // On YARN mode, the following preprocessed PYTHONPATH will be added to the PYTHONPATH
      // of the YARN container processes
      if (contextSparkMaster.contains("yarn")) {
        val pythonPath = new ListBuffer[String]()
        getStringSeq(contextConfig, "launcher.py-files").foreach { path =>
          val uri = Utils.resolveURI(path)
          val fname = new Path(uri).getName
          if (!fname.endsWith(".py")) {
            if (uri.getScheme != "local") {
              pythonPath += ApplicationConstants.Environment.PWD.$$() + Path.SEPARATOR + fname
            } else {
              pythonPath += uri.getPath
            }
          }
        }
        env.put("PYTHONPATH", pythonPath.mkString(ApplicationConstants.CLASS_PATH_SEPARATOR))
      }
      // If the Spark driver is not in cluster, use the PYTHONPATH of client process
      val sjsPythonPath = environment.get("PYTHONPATH", "")
      env.put("SJSPYTHONPATH", sjsPythonPath)
    }
    val sparkLauncher = new SparkLauncher(env.asJava)
    sparkLauncher.setMaster(contextSparkMaster)
    new ManagerLauncher(config, contextConfig, masterAddress,
      contextName, contextActorName, contextDir.toString, sparkLauncher)
  }
}

class ManagerLauncher(systemConfig: Config, contextConfig: Config, masterAddress: String,
                      contextName: String, contextActorName: String, contextDir: String,
                      sparkLauncher: SparkLauncher = new SparkLauncher,
                      environment: Environment = new SystemEnvironment)
  extends Launcher(systemConfig, sparkLauncher, environment) {
  val logger = LoggerFactory.getLogger("spark-context-launcher")

  var loggingOpts = getEnvironmentVariable("MANAGER_LOGGING_OPTS")
  val configOverloads = getEnvironmentVariable("CONFIG_OVERRIDES")
  val extraSparkConfigurations = getEnvironmentVariable("MANAGER_EXTRA_SPARK_CONFS")
  val extraJavaOPTS = getEnvironmentVariable("MANAGER_EXTRA_JAVA_OPTIONS")
  var gcOPTS = baseGCOPTS
  lazy val contextSuperviseModeEnabled: Option[Boolean] = Try(Some(
    contextConfig.getBoolean(ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY))).getOrElse(None)
  lazy val useSuperviseMode = ManagerLauncher.shouldSuperviseModeBeEnabled(
    defaultSuperviseModeEnabled, contextSuperviseModeEnabled)

  override def addCustomArguments() {
    val gcFileName = getEnvironmentVariable("GC_OUT_FILE_NAME", "gc.out")
    if (deployMode == "client") {
      val gcFilePath = new File(contextDir, gcFileName).toString()
      loggingOpts += s" -DLOG_DIR=$contextDir"
      gcOPTS += s" -Xloggc:$gcFilePath"
    } else {
      gcOPTS += s" -Xloggc:$gcFileName"
    }

    launcher.setMainClass("spark.jobserver.JobManager")
    launcher.addAppArgs(masterAddress, contextActorName, getEnvironmentVariable("MANAGER_CONF_FILE"))
    launcher.addSparkArg("--conf", s"spark.executor.extraJavaOptions=$loggingOpts")
    launcher.addSparkArg("--driver-java-options",
      s"$gcOPTS $baseJavaOPTS $loggingOpts $configOverloads $extraJavaOPTS")

    if (!contextName.isEmpty) {
      launcher.setAppName(contextName)
    }

    if (useSuperviseMode) {
      launcher.addSparkArg("--supervise")
    }

    if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
      launcher.addSparkArg("--proxy-user", contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
    }

    getStringSeq(contextConfig, "jars").foreach(launcher.addJar)
    getStringSeq(contextConfig, "files").foreach(launcher.addFile)

    if (contextConfig.getString("context-factory").contains("python")) {
      // Since `--py-files` in not available in Java applications,
      // we distribute pyFiles to cluster var `--files`
      launcher.setConf("spark.yarn.isPython", "true")
      val pyFiles = getStringSeq(contextConfig, "launcher.py-files")
      pyFiles.foreach(launcher.addFile)
      // If Spark driver is local, we need to download the remote pyFiles.
      // Ignoring pyFiles in yarn and mesos cluster mode, these two modes support dealing
      // with remote pyFiles, they could distribute and add pyiles locally.
      if (deployMode != "cluster" || !master.startsWith("yarn")) {
        val hdfs = new HadoopFSFacade()
        val tmpDir = Files.createTempDirectory(s"jobserver-$contextName")
        val localPyFiles = pyFiles.map(f => hdfs.downloadFile(f, tmpDir.toFile))
        launcher.setConf("spark.jobserver.tmpDir", tmpDir.toString)
        launcher.setConf("spark.submit.pyFiles", localPyFiles.mkString(","))
      }
    }

    for (e <- Try(contextConfig.getConfig("launcher"))) {
      e.entrySet().asScala.map { c =>
        // Supervise mode was already handled above, no need to do it here again
        if (c.getKey.startsWith("spark") && c.getKey != "spark.driver.supervise") {
          launcher.addSparkArg("--conf", s"${c.getKey}=${c.getValue.unwrapped.toString}")
        }
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
        if (!validateMemory(contextConfig.getString("launcher.spark.driver.memory"))) {
          (false,
            "Context error: spark.driver.memory has invalid value. Accepted formats 1024k, 2g, 512m")
        } else if (deployMode == "client" && useSuperviseMode) {
          (false, "Supervise mode can only be used with cluster mode")
        } else if (master.startsWith("yarn") && useSuperviseMode) {
          (false, "Supervise mode is only supported with spark standalone or Mesos")
        } else {
          (true, "")
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
