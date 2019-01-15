package spark.jobserver.io.zookeeper

import org.joda.time.DateTime
import spark.jobserver.io.{BinaryInfo, BinaryType}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import spark.jobserver.util.{CuratorTestCluster, JsonProtocols}

class ZookeeperUtilsSpec extends FunSpec with Matchers with BeforeAndAfter{
  private val testPath = "sjstest"
  private val testServer = new CuratorTestCluster()
  private val zookeeperUtils = new ZookeeperUtils(testServer.getConnectString, testPath, 1)
  private val testInfo = BinaryInfo("test", BinaryType.Jar, DateTime.now())

  import JsonProtocols._

  before {
    testServer.createBaseDir(testPath)
    zookeeperUtils.delete("/")
  }

  describe("write, read and delete data") {
    it("should write, list and delete object") {
      zookeeperUtils.write[BinaryInfo](testInfo, "/testfile") should equal (true)
      zookeeperUtils.list("/") should equal(Seq("testfile"))
      zookeeperUtils.delete("/testfile") should equal (true)
      zookeeperUtils.list("/") should equal(Seq.empty[String])
    }

    it("should read object") {
      zookeeperUtils.write[BinaryInfo](testInfo, "/testfile")
      val readInfo = zookeeperUtils.read[BinaryInfo]("/testfile").get
      readInfo should equal (testInfo)
    }

    it("should return None if there is no required object") {
      zookeeperUtils.read[BinaryInfo]("/testfile") should equal (None)
    }

    it("should return None if object is not parsable") {
      zookeeperUtils.write[BinaryInfo](testInfo, "/testfile")
      zookeeperUtils.read[Int]("/testfile") should equal (None)
    }
  }

  describe("list Zookeeper content") {
    it("should list objects") {
      zookeeperUtils.write[BinaryInfo](testInfo, "/testfile1")
      zookeeperUtils.write[BinaryInfo](testInfo, "/testfile2")
      zookeeperUtils.write[BinaryInfo](testInfo, "/testfile3")
      zookeeperUtils.list("/").sorted should equal(Seq("testfile1", "testfile2", "testfile3"))
    }

    it("should return empty list if there are no children") {
      zookeeperUtils.list("/").sorted should equal(Seq())
    }
  }

  describe("test zookeeper connection") {
    it("should return false for write operation if connection is lost") {
      new ZookeeperUtils("ip_address_doesnt_exist", testPath, 1).
        write[BinaryInfo](testInfo, "/testfile1") should equal(false)
    }

    it("should not see other namespaces") {
      zookeeperUtils.list("/").sorted should equal(Seq())
      new ZookeeperUtils(testServer.getConnectString, "", 1).
        list("/") should equal (Seq("zookeeper", testPath))
    }
  }
}
