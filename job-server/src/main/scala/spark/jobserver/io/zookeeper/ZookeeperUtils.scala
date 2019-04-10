package spark.jobserver.io.zookeeper

import com.typesafe.config.Config
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.{BackgroundCallback, CuratorEvent}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.utils.ZKPaths
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.util.Utils
import spray.json._

import scala.util.control.NonFatal

// Fix type mismatch: java.util.List[String] in curator results
import scala.collection.JavaConversions._ // scalastyle:ignore

object ZookeeperUtils {
  private val settings = Map(
    "baseFolder" -> "spark.jobserver.zookeeperdao.dir",
    "connectionString" -> "spark.jobserver.zookeeperdao.connection-string",
    "retries" -> "spark.jobserver.zookeeperdao.curator.retries",
    "sleepBetweenRetries" -> "spark.jobserver.zookeeperdao.curator.sleepMsBetweenRetries",
    "connectionTimeout" -> "spark.jobserver.zookeeperdao.curator.connectionTimeoutMs",
    "sessionTimeout" -> "spark.jobserver.zookeeperdao.curator.sessionTimeoutMs"
  )
}

class ZookeeperUtils(config: Config) {
  import ZookeeperUtils._

  private val logger = LoggerFactory.getLogger(getClass)

  for (path <- settings.values) {
    if (!config.hasPath(path)) {
      throw new InvalidConfiguration(
        s"To use ZookeeperDAO please specify $path in configuration file."
      )
    }
  }

  private val connectionString = config.getString(settings("connectionString"))
  private val baseFolder = config.getString(settings("baseFolder"))
  private val retries = config.getInt(settings("retries"))
  private val connectionTimeout = config.getInt(settings("connectionTimeout"))
  private val sleepBetweenRetries = config.getInt(settings("sleepBetweenRetries"))
  private val sessionTimeout = config.getInt(settings("sessionTimeout"))

  def getClient: CuratorFramework = {
    val client = CuratorFrameworkFactory.builder.
      connectString(connectionString).
      retryPolicy(new RetryNTimes(retries, sleepBetweenRetries)).
      namespace(baseFolder).
      connectionTimeoutMs(connectionTimeout).
      sessionTimeoutMs(sessionTimeout).
      build
    client.start()
    client
  }

  def list(client: CuratorFramework, dir: String): Seq[String] = {
    logger.debug("List path $dir")
    if (client.checkExists().forPath(dir) == null) {
      Seq.empty[String]
    } else {
      client.getChildren.forPath(dir).toList
    }
  }

  def sync(client: CuratorFramework, path: String): Unit = {
    logger.debug(s"Starting a sync for $path")
    val callback = new BackgroundCallback(){
      def processResult(client : CuratorFramework, event : CuratorEvent) {
        logger.debug(s"Sync for $path completed.")
      }
    }
    client.sync().inBackground(callback).forPath(path)
  }

  def read[T: JsonReader](client: CuratorFramework, path: String): Option[T] = {
    logger.debug(s"Reading from $path")
    try {
      if (client.checkExists().forPath(path) != null) {
        val bytes = client.getData.forPath(path)
        val jsonAst = bytes.map(_.toChar).mkString.parseJson
        Some(jsonAst.convertTo[T])
      } else {
        None
      }
    } catch {
      case NonFatal(e) =>
        Utils.logStackTrace(logger, e)
        None
    }
  }

  def write[T: JsonWriter](client: CuratorFramework, data: T, path: String): Boolean = {
    logger.debug(s"Writing to $path")
    try {
      if (client.checkExists().forPath(path) == null) {
        logger.info(s"Directory $path doesn't exists. Making dirs.")
        ZKPaths.mkdirs(client.getZookeeperClient.getZooKeeper, ZKPaths.fixForNamespace(baseFolder, path))
      }
      client.setData().forPath(path, data.toJson.compactPrint.toCharArray.map(_.toByte))
      true
    } catch {
      case NonFatal(e) =>
        Utils.logStackTrace(logger, e)
        false
    }
  }

  def delete(client: CuratorFramework, dir: String): Boolean = {
    logger.debug(s"Deleting from $dir")
    try {
      if (client.checkExists().forPath(dir) != null) {
        ZKPaths.deleteChildren(client.getZookeeperClient.getZooKeeper,
          ZKPaths.fixForNamespace(baseFolder, dir),
          false)
        client.delete().forPath(dir)
      } else {
        logger.info(s"Directory $dir doesn't exist. Nothing to delete.")
      }
      true
    } catch {
      case NonFatal(e) =>
        Utils.logStackTrace(logger, e)
        false
    }
  }
}
