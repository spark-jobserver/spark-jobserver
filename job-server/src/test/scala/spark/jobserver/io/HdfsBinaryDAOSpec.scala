package spark.jobserver.io

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import spark.jobserver.util.HadoopFSFacade
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.util.HDFSClusterLike


import scala.concurrent.Await
import scala.concurrent.duration._

class HdfsBinaryDAOSpec extends FunSpec with Matchers with BeforeAndAfterAll with HDFSClusterLike {
  private var hdfsDAO: HdfsBinaryDAO = _
  private var testDir: String = _
  private val testFileName = "test_file"
  private val timeout = 30 seconds

  override def beforeAll: Unit = {
    super.startHDFS()
    val testClusterUrl = getNameNodeURI()
    testDir = s"$testClusterUrl/binarydao-test"
    def config: Config = ConfigFactory.parseString(
      s"""spark.jobserver.combineddao.binarydao.dir = "$testDir""""
    )
    hdfsDAO = new HdfsBinaryDAO(config)
  }

  override def afterAll(): Unit = {
    new HadoopFSFacade().delete(testDir, recursive = true) // cleanup artifacts
    super.shutdownHDFS()
  }

  describe("check config validation in constructor") {
    it("should throw InvalidConfiguration if hdfs dir is missing in config") {
      assertThrows[InvalidConfiguration] {
        new HdfsBinaryDAO(ConfigFactory.empty())
      }
    }
  }

  describe("write, get, delete binary data") {
    it("should write, get, delete the file from hdfs") {
      val testArray: Array[Byte] = Array(1, 4, 4, 1)
      Await.result(
        hdfsDAO.get(testFileName), timeout) should equal (None) // ensure there is no file before
      Await.result(hdfsDAO.save(testFileName, testArray), timeout) should equal (true)
      Await.result(hdfsDAO.get(testFileName), timeout).get should equal (testArray)
      Await.result(hdfsDAO.delete(testFileName), timeout) should equal (true)
      Await.result(
        hdfsDAO.get(testFileName), timeout) should equal (None) // ensure there is no file after
    }

    it("should return nothing if file doesn't exist") {
      Await.result(hdfsDAO.get(testFileName), timeout) should equal (None)
    }

    it("should return false if delete is unsuccessful") {
      Await.result(hdfsDAO.delete(testFileName), timeout) should equal (false)
    }

    it("should return true writing a file which already exists") {
      val testArray: Array[Byte] = "Test file".toCharArray.map(_.toByte)
      Await.result(hdfsDAO.save(testFileName, testArray), timeout) should equal (true)
      Await.result(hdfsDAO.save(testFileName, testArray), timeout) should equal (true)
    }
  }

  describe("check behavior if HDFS is misconfigured") {
    it("should return false if save is unsuccessful") {
      def config: Config = ConfigFactory.parseString(
        s"""spark.jobserver.combineddao.binarydao.dir = "hdfs://foo:foo/binarydao-test""""
      )
      hdfsDAO = new HdfsBinaryDAO(config)
      Await.result(hdfsDAO.delete(testFileName), timeout) should equal(false)
    }
  }
}
