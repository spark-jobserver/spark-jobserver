package spark.jobserver.io.zookeeper

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.utils.ZKPaths
import org.slf4j.LoggerFactory
import spark.jobserver.util.Utils
import spray.json._

import scala.util.control.NonFatal

// Fix type mismatch: java.util.List[String] in curator results
import scala.collection.JavaConversions._ // scalastyle:ignore


class ZookeeperUtils(connectString: String, baseFolder: String, retries: Int = 5) {
  private val logger = LoggerFactory.getLogger(getClass)

  def getClient: CuratorFramework = {
      val client = CuratorFrameworkFactory.builder.
        connectString(connectString).
        retryPolicy(new RetryNTimes(retries, 500)).
        namespace(baseFolder).
        connectionTimeoutMs(1000).
        build
      client.start()
      client
  }

  def list(dir: String): Seq[String] = {
    Utils.usingResource(getClient) {
      client =>
        if (client.checkExists().forPath(dir) == null) {
          Seq.empty[String]
        } else {
          client.getChildren.forPath(dir).toList
        }
    }
  }

  def read[T: JsonReader](path: String): Option[T] = {
    Utils.usingResource(getClient) {
      client =>
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
  }

  def write[T: JsonWriter](data: T, path: String): Boolean = {
    Utils.usingResource(getClient) {
      client =>
        try {
          if (client.checkExists().forPath(path) != null) {
            logger.info(s"Directory $path already exists. Setting new data value.")
          } else {
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
  }

  def delete(dir: String): Boolean = {
    Utils.usingResource(getClient) {
      client =>
        try {
          if (client.checkExists().forPath(dir) != null) {
            ZKPaths.deleteChildren(
              client.getZookeeperClient.getZooKeeper,
              ZKPaths.fixForNamespace(baseFolder, dir),
              false
            )
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
}
