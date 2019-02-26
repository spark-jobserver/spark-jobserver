package spark.jobserver.io.zookeeper

import org.joda.time.DateTime
import spark.jobserver.io.{BinaryInfo, BinaryType}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import spark.jobserver.util.{CuratorTestCluster, JsonProtocols}
import org.apache.curator.framework.CuratorFramework

class ZookeeperUtilsSpec extends FunSpec with Matchers with BeforeAndAfter {
  private val testPath = "sjstest"
  private val testServer = new CuratorTestCluster()
  private val zookeeperUtils = new ZookeeperUtils(testServer.getConnectString, testPath, 1)
  private val testInfo = BinaryInfo("test", BinaryType.Jar, DateTime.now())
  private var client: CuratorFramework = _

  import JsonProtocols._

  before {
    testServer.createBaseDir(testPath)
    client = zookeeperUtils.getClient
    zookeeperUtils.delete(client, "/")
  }

  after {
    client.close()
  }

  describe("write, read and delete data") {
    it("should write, list and delete object") {
      zookeeperUtils.write[BinaryInfo](client, testInfo, "/testfile") should equal(true)
      zookeeperUtils.list(client, "/") should equal(Seq("testfile"))
      zookeeperUtils.delete(client, "/testfile") should equal(true)
      zookeeperUtils.list(client, "/") should equal(Seq.empty[String])
    }

    it("should read object") {
      zookeeperUtils.write[BinaryInfo](client, testInfo, "/testfile")
      val readInfo = zookeeperUtils.read[BinaryInfo](client, "/testfile").get
      readInfo should equal(testInfo)
    }

    it("should return None if there is no required object") {
      zookeeperUtils.read[BinaryInfo](client, "/testfile") should equal(None)
    }

    it("should return None if object is not parsable") {
      zookeeperUtils.write[BinaryInfo](client, testInfo, "/testfile")
      zookeeperUtils.read[Int](client, "/testfile") should equal(None)
    }
  }

  describe("list Zookeeper content") {
    it("should list objects") {
      zookeeperUtils.write[BinaryInfo](client, testInfo, "/testfile1")
      zookeeperUtils.write[BinaryInfo](client, testInfo, "/testfile2")
      zookeeperUtils.write[BinaryInfo](client, testInfo, "/testfile3")
      zookeeperUtils.list(client, "/").sorted should equal(Seq("testfile1", "testfile2", "testfile3"))
    }

    it("should return empty list if there are no children") {
      zookeeperUtils.list(client, "/").sorted should equal(Seq())
    }
  }

  describe("test zookeeper connection") {
    it("should return false for write operation if connection is lost") {
      val zkUtils2 = new ZookeeperUtils("ip_address_doesnt_exist", testPath, 1)
      zkUtils2.write[BinaryInfo](zkUtils2.getClient, testInfo, "/testfile1") should equal(false)
    }

    it("should not see other namespaces") {
      zookeeperUtils.list(client, "/").sorted should equal(Seq())
      val zkUtils2 = new ZookeeperUtils(testServer.getConnectString, "", 1)
      zkUtils2.list(zkUtils2.getClient, "/") should equal(Seq("zookeeper"))
    }
  }
}
