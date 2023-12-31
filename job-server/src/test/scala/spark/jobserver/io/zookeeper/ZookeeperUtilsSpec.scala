package spark.jobserver.io.zookeeper

import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.io.{BinaryInfo, BinaryType}
import org.scalatest.BeforeAndAfter
import spark.jobserver.util.{CuratorTestCluster, JsonProtocols, Utils}
import org.apache.curator.framework.CuratorFramework
import spark.jobserver.JobServer.InvalidConfiguration

import java.time.ZonedDateTime
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ZookeeperUtilsSpec extends AnyFunSpec with Matchers with BeforeAndAfter {
  private val testServer = new CuratorTestCluster()

  def config: Config = ConfigFactory.parseString(
    s"""
       |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}",
       |""".stripMargin
  ).withFallback(
    ConfigFactory.load("local.test.dao.conf")
  )

  private val zookeeperUtils = new ZookeeperUtils(config)
  private val testInfo = BinaryInfo("test", BinaryType.Jar, ZonedDateTime.now().withNano(0))
  private var client: CuratorFramework = _

  import JsonProtocols._

  before {
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
      def modifiedConfig: Config = ConfigFactory.parseString(
        "spark.jobserver.zookeeperdao.connection-string = ip_address_doesnt_exist"
      )
      val zkUtils2 = new ZookeeperUtils(modifiedConfig.withFallback(config))
      Utils.usingResource(zkUtils2.getClient) {
        client =>
          zkUtils2.write[BinaryInfo](client, testInfo, "/testfile1") should equal(false)
      }
    }

    it("should not see other namespaces") {
      zookeeperUtils.list(client, "/").sorted should equal(Seq())
      def modifiedConfig: Config = ConfigFactory.parseString(
        """
          spark.jobserver.zookeeperdao.dir = ""
        """
      )
      val zkUtils2 = new ZookeeperUtils(modifiedConfig.withFallback(config))
      Utils.usingResource(zkUtils2.getClient) {
        client =>
          zkUtils2.list(client, "/") should equal(Seq("zookeeper"))
      }
    }
  }

  describe("should validate that configuration has all parameteres") {
    it("should throw InvalidConfiguration if zookeeper connection string is missing in config") {
      assertThrows[InvalidConfiguration] {
        new ZookeeperUtils (
          config.withoutPath("spark.jobserver.zookeeperdao.connection-string")
        )
      }
    }

    it("should throw InvalidConfiguration if zookeeper directory string is missing in config") {
      assertThrows[InvalidConfiguration] {
        new ZookeeperUtils (
          config.withoutPath("spark.jobserver.zookeeperdao.dir")
        )
      }
    }
  }
}
