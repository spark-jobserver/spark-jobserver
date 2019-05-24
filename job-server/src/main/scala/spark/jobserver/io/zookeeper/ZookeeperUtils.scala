package spark.jobserver.io.zookeeper

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.{BackgroundCallback, CuratorEvent}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.utils.ZKPaths
import org.slf4j.LoggerFactory
import spray.json._

import scala.util.control.NonFatal

// Fix type mismatch: java.util.List[String] in curator results
import scala.collection.JavaConversions._ // scalastyle:ignore

class ZookeeperUtils(connectString: String, baseFolder: String, retries: Int = 3) {
  private val logger = LoggerFactory.getLogger(getClass)

  def getClient: CuratorFramework = {
    val client = CuratorFrameworkFactory.builder.
      connectString(connectString).
      retryPolicy(new RetryNTimes(retries, 1000)).
      namespace(baseFolder).
      connectionTimeoutMs(2350).
      sessionTimeoutMs(10000).
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
        logger.error(e.getMessage)
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
        logger.error(e.getMessage)
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
        logger.error(e.getMessage)
        false
    }
  }
}
