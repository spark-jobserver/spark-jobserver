package spark.jobserver.util

import org.apache.curator.test.TestingServer
import org.apache.curator.utils.ZKPaths
import org.slf4j.LoggerFactory
import spark.jobserver.io.zookeeper.ZookeeperUtils

import scala.util.control.NonFatal

class CuratorTestCluster {
  private val logger = LoggerFactory.getLogger(getClass)
  private val server: TestingServer = new TestingServer()

  sys.addShutdownHook {
    server.close()
  }

  def getConnectString: String = server.getConnectString

  def createBaseDir(basePath: String): Boolean = {
    Utils.usingResource(new ZookeeperUtils(getConnectString, basePath, 1).getClient) {
      client =>
        try {
          if (client.checkExists().forPath(basePath) == null) {
            ZKPaths.mkdirs(
              client.getZookeeperClient.getZooKeeper, basePath
            )
          }
          true
        } catch {
          case NonFatal(e) =>
            logger.error(s"Didn't manage to create dir $basePath: ${e.getMessage}")
            false
        }
    }
  }
}
