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
}
