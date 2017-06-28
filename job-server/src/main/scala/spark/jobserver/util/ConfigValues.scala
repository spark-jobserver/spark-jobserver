package spark.jobserver.util

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

object ConfigValues {

  private val logger        = LoggerFactory.getLogger(getClass)
  private var currentConfig = ConfigFactory.load()

  final val CFG_CLASS: String        = "com.typesafe.config.Config"
  final val PORT: Int                = config.getInt("spark.jobserver.port")
  final val JOB_DAO: String          = config.getString("spark.jobserver.jobdao")
  final val CONTEXT_PER_JVM: Boolean = config.getBoolean("spark.jobserver.context-per-jvm")
  final val SQL_DAO_JDBC_URL: String = config.getString("spark.jobserver.sqldao.jdbc.url")
  final val LEGACY_JAR_PATH: Config  = configOpt("spark.jobserver.job-jar-paths")
  final val BIN_PATH: Config         = configOpt("spark.jobserver.job-bin-paths")

  final def config(args: Array[String] = Array.empty[String]): Config = {
    if (args.nonEmpty) {
      val configFile = new File(args.head)
      if (!configFile.exists()) {
        logger.error("Could not find configuration file " + configFile)
        sys.exit(1)
      }
      currentConfig = ConfigFactory.parseFile(configFile).withFallback(currentConfig).resolve()
    }
    currentConfig
  }

  final def config: Config = config()

  final def configOpt(key: String): Config = {
    if (config.hasPath(key)) {
      config.getConfig(key)
    } else {
      ConfigFactory.empty()
    }
  }
}
