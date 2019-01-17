package spark.jobserver.io.zookeeper

import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import spark.jobserver.io.{BinaryInfo, BinaryType}
import spark.jobserver.util.CuratorTestCluster

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class MetaDataZookeeperDAOSpec  extends FunSpec with Matchers with BeforeAndAfter {
  private val testServer = new CuratorTestCluster()
  private val testDir = "jobserver-test"
  private val timeout = 60 seconds

  def config: Config = ConfigFactory.parseString(
    s"""
       |spark.jobserver.zookeeper.connection-string = "${testServer.getConnectString}",
       |spark.jobserver.zookeeper.dir = $testDir""".stripMargin
  )

  private val dao = new MetaDataZookeeperDAO(config)
  private val zkUtils = new ZookeeperUtils(testServer.getConnectString, testDir, 1)

  before {
    testServer.createBaseDir(testDir)
    zkUtils.delete("/")
  }

  describe("binaries functions") {
    val testInfo = BinaryInfo("test", BinaryType.Jar, DateTime.now(), Some("storageid"))

    it("should save binary info") {
      Await.result(dao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout) should equal (true)
      zkUtils.list("/binaries") should equal (Seq(testInfo.appName))
    }

    it("should always return last uploaded BinaryInfo for the given name") {
      Await.result(dao.saveBinary(testInfo.appName, BinaryType.Jar, testInfo.uploadTime, "v1"), timeout)
      val lastInfo = BinaryInfo("test", BinaryType.Jar, DateTime.now(), Some("storageid"))
      Await.result(dao.saveBinary(lastInfo.appName, lastInfo.binaryType,
        lastInfo.uploadTime, lastInfo.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(testInfo.appName, BinaryType.Jar, testInfo.uploadTime, "v2"), timeout)
      Await.result(dao.getBinary(testInfo.appName), timeout).get should equal (lastInfo)
    }

    it("should return None if there is no binary with a given name") {
      Await.result(dao.getBinary(testInfo.appName), timeout) should equal (None)
    }

    it("should delete binary info") {
      Await.result(dao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout) should equal (true)
      zkUtils.list("/binaries") should equal (Seq(testInfo.appName))
      Await.result(dao.deleteBinary(testInfo.appName), timeout) should equal (true)
      zkUtils.list("/binaries") should equal (Seq.empty[String])
    }

    it("should return false trying delete not existing object") {
      Await.result(dao.deleteBinary(testInfo.appName), timeout) should equal (false)
    }

    it("should list all binaries with given storage id") {
      val testInfoCopy = BinaryInfo("test1", BinaryType.Jar, DateTime.now(), Some("storageid"))
      Await.result(dao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(testInfoCopy.appName, testInfoCopy.binaryType,
        testInfoCopy.uploadTime, testInfoCopy.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(testInfo.appName + "2", testInfo.binaryType,
        testInfo.uploadTime, "anotherStorageId"), timeout)
      Await.result(
        dao.getBinariesByStorageId(testInfo.binaryStorageId.get),
        timeout).sortBy(_.appName) should equal (Seq(testInfo, testInfoCopy))
    }

    it("should list all binaries with given storage id with unique (name, binaryStorageId)") {
      Await.result(dao.saveBinary(testInfo.appName, testInfo.binaryType,
        DateTime.now(), testInfo.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(testInfo.appName + "2", testInfo.binaryType,
        testInfo.uploadTime, "anotherStorageId"), timeout)
      val resultList = Await.result(dao.getBinariesByStorageId(testInfo.binaryStorageId.get), timeout)
      resultList.length should equal(1)
      resultList.head.appName should equal(testInfo.appName)
    }

    it("should list all existing binaries") {
      val anotherTestInfo = BinaryInfo("test1", BinaryType.Jar, DateTime.now(), Some("anotherId"))
      Await.result(dao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime.minus(1000), "someId"), timeout)
      Await.result(dao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(anotherTestInfo.appName, anotherTestInfo.binaryType,
        anotherTestInfo.uploadTime, anotherTestInfo.binaryStorageId.get), timeout)
      Await.result(dao.getBinaries, timeout) should equal (Seq(testInfo, anotherTestInfo))
    }

    it("should return empty list if there are no binaries to list") {
      Await.result(dao.getBinaries, timeout) should equal (Seq.empty[BinaryInfo])
    }
  }

  // TODO: zk exceptions mock (save, delete, list)
}
