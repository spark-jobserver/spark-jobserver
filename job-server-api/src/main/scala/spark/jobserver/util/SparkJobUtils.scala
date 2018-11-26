package spark.jobserver.util

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import org.apache.spark.SparkConf

import scala.util.{Try, Success}

/**
 * Holds a few functions common to Job Server SparkJob's and SparkContext's
 */
object SparkJobUtils {
  import collection.JavaConverters._
  val NameContextDelimiter = "~"

  /**
   * User impersonation for an already Kerberos authenticated user is supported via the
   * `spark.proxy.user` query param
   */
  val SPARK_PROXY_USER_PARAM = "spark.proxy.user"

  private val regRexPart2 = "([^" + SparkJobUtils.NameContextDelimiter + "]+.*)"

  /**
    * appends the NameContextDelimiter to the user name and,
    * if the user name contains the delimiter as well, then it doubles it so that we can be sure
    * that our prefix is unique
    */
  def userNamePrefix(userName: String) : String = {
    userName.replaceAll(NameContextDelimiter,
      NameContextDelimiter + NameContextDelimiter) +
      NameContextDelimiter
  }

  /**
    * filter the given context names so that the user may only see his/her own contexts
    */
  def removeProxyUserPrefix(userName: => String, contextNames: Seq[String], filter: Boolean): Seq[String] = {
    if (filter) {
      val RegExPrefix = ("^" + userNamePrefix(userName) + regRexPart2).r
      contextNames collect {
        case RegExPrefix(cName) => cName
      }
    } else {
      contextNames
    }
  }

  /**
   * Creates a SparkConf for initializing a SparkContext based on various configs.
   * Note that anything in contextConfig with keys beginning with spark. get
   * put directly in the SparkConf.
   *
   * @param config the overall Job Server configuration (Typesafe Config)
   * @param contextConfig the Typesafe Config specific to initializing this context
   *                      (typically based on particular context/job)
   * @param contextName the context name
   * @return a SparkConf with everything properly configured
   */
  def configToSparkConf(config: Config, contextConfig: Config,
                        contextName: String): SparkConf = {
    val conf = new SparkConf()
    conf.setAppName(contextName)

    Try(conf.get("spark.master")) match {
      case Success(value) => // If Launcher has already set the value then don't override
      case _ =>
        val sparkMaster = SparkMasterProvider.fromConfig(config).getSparkMaster(config)
        conf.setMaster(sparkMaster)
    }

    for (cores <- Try(contextConfig.getInt("num-cpu-cores"))) {
      conf.set("spark.cores.max", cores.toString)
    }
    // Should be a -Xmx style string eg "512m", "1G"
    for (nodeMemStr <- Try(contextConfig.getString("memory-per-node"))) {
      conf.set("spark.executor.memory", nodeMemStr)
    }

    Try(config.getString("spark.home")).foreach { home => conf.setSparkHome(home) }

    // Set the Jetty port to 0 to find a random port
    conf.set("spark.ui.port", "0")

    // Set number of akka threads
    // TODO: need to figure out how many extra threads spark needs, besides the job threads
    conf.set("spark.akka.threads", (getMaxRunningJobs(config) + 4).toString)

    // Set any other settings in context config that start with "spark"
    for (e <- contextConfig.entrySet().asScala if e.getKey.startsWith("spark.")) {
      conf.set(e.getKey, e.getValue.unwrapped.toString)
    }

    // Set any other settings in context config that start with "passthrough"
    // These settings will be directly set in sparkConf, but with "passthrough." stripped
    // This is useful for setting configurations for hadoop connectors such as
    // elasticsearch, cassandra, etc.
    for (e <- Try(contextConfig.getConfig("passthrough"))) {
         e.entrySet().asScala.map { s =>
            conf.set(s.getKey, s.getValue.unwrapped.toString)
         }
    }

    conf
  }

  /**
    *
    * @param config the specific context configuration
    * @return a map of the hadoop configuration values or an empty Map
    */
  def getHadoopConfig(config: Config): Map[String, String] = {
    Try(config.getConfig("hadoop").entrySet().asScala.map { e =>
      e.getKey -> e.getValue.unwrapped().toString
    }.toMap).getOrElse(Map())
  }

  /**
   * Returns the maximum number of jobs that can run at the same time
   */
  def getMaxRunningJobs(config: Config): Int = {
    val cpuCores = Runtime.getRuntime.availableProcessors
    Try(config.getInt("spark.jobserver.max-jobs-per-context")).getOrElse(cpuCores)
  }

  private def getContextTimeout(config: Config, yarn : String, standalone : String): Int = {
    config.getString("spark.master") match {
      case "yarn" =>
        Try(config.getDuration(yarn,
              TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(40)
      case _ =>
        Try(config.getDuration(standalone,
              TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(15)
    }
  }

  /**
    * According "spark.master", returns the timeout of create sparkContext
    */
  def getContextCreationTimeout(config: Config): Int = {
    getContextTimeout(config, "spark.jobserver.yarn-context-creation-timeout",
        "spark.jobserver.context-creation-timeout")
    }

  /**
    * According "spark.master", returns the timeout of delete sparkContext
    */
  def getContextDeletionTimeout(config: Config): Int = {
    getContextTimeout(config, "spark.jobserver.yarn-context-deletion-timeout",
      "spark.jobserver.context-deletion-timeout")
  }

  def getForkedJVMInitTimeout(config: Config): Long = {
    config.getDuration("spark.context-settings.forked-jvm-init-timeout", TimeUnit.SECONDS)
  }
}
