package spark.jobserver.io

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import spark.jobserver.util.HadoopFSFacade
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.util.HDFSCluster

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class HdfsBinaryObjectsDAOSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with HDFSCluster {
  private var hdfsDAO: HdfsBinaryObjectsDAO = _
  private var testDir: String = _
  private val testFileName = "test_file"
  private val timeout = 30 seconds

  override def beforeAll: Unit = {
    super.startHDFS()
    val testClusterUrl = getNameNodeURI()
    testDir = s"$testClusterUrl/binarydao-test"
    def config: Config = ConfigFactory.parseString(
      s"""spark.jobserver.binarydao.dir = "$testDir""""
    )
    hdfsDAO = new HdfsBinaryObjectsDAO(config)
  }

  override def afterAll(): Unit = {
    new HadoopFSFacade().delete(testDir, recursive = true) // cleanup artifacts
    super.shutdownHDFS()
  }

  describe("check config validation in constructor") {
    it("should throw InvalidConfiguration if hdfs dir is missing in config") {
      assertThrows[InvalidConfiguration] {
        new HdfsBinaryObjectsDAO(ConfigFactory.empty())
      }
    }
  }

  describe("write, get, delete binary data") {
    ignore("should write, get, delete the file from hdfs") {
      val validInvalidInput = "Ḽơᶉëᶆ ȋṕšᶙṁ � � � � test 1 4 4 1".toCharArray.map(_.toByte)
      Await.result(
        hdfsDAO.getBinary(testFileName), timeout) should equal (None) // ensure there is no file before
      Await.result(hdfsDAO.saveBinary(testFileName, validInvalidInput), timeout) should equal (true)
      Await.result(hdfsDAO.getBinary(testFileName), timeout).get should equal (validInvalidInput)
      Await.result(hdfsDAO.deleteBinary(testFileName), timeout) should equal (true)
      Await.result(
        hdfsDAO.getBinary(testFileName), timeout) should equal (None) // ensure there is no file after
    }

    it("should return nothing if file doesn't exist") {
      Await.result(hdfsDAO.getBinary(testFileName), timeout) should equal (None)
    }

    it("should return false if delete is unsuccessful") {
      Await.result(hdfsDAO.deleteBinary(testFileName), timeout) should equal (false)
    }

    ignore("should return true writing a file which already exists") {
      val testArray: Array[Byte] = "Test file".toCharArray.map(_.toByte)
      Await.result(hdfsDAO.saveBinary(testFileName, testArray), timeout) should equal (true)
      Await.result(hdfsDAO.saveBinary(testFileName, testArray), timeout) should equal (true)
    }
  }

  describe("write, get, delete job results data") {
    ignore("should write, get, delete the file from hdfs") {
      val validInvalidInput = "Ḽơᶉëᶆ ȋṕšᶙṁ � � � � test 1 4 4 1".toCharArray.map(_.toByte)
      Await.result(
        hdfsDAO.getJobResult(testFileName), timeout) should equal(None) // ensure there is no file before
      Await.result(hdfsDAO.saveJobResult(testFileName, validInvalidInput), timeout) should equal(true)
      Await.result(hdfsDAO.getJobResult(testFileName), timeout).get should equal(validInvalidInput)
      Await.result(hdfsDAO.deleteJobResults(Seq(testFileName)), timeout) should equal(true)
      Await.result(
        hdfsDAO.getJobResult(testFileName), timeout) should equal(None) // ensure there is no file after
    }

    it("should return nothing if file doesn't exist") {
      Await.result(hdfsDAO.getJobResult(testFileName), timeout) should equal(None)
    }

    ignore("should delete multiple job results") {
      val result = "Result".toCharArray.map(_.toByte)
      Await.result(hdfsDAO.saveJobResult("1", result), timeout) should equal(true)
      Await.result(hdfsDAO.saveJobResult("2", result), timeout) should equal(true)
      Await.result(hdfsDAO.saveJobResult("3", result), timeout) should equal(true)
      Await.result(hdfsDAO.deleteJobResults(Seq("1", "2")), timeout) should equal(true)
      Await.result(hdfsDAO.getJobResult("1"), timeout) should equal(None)
      Await.result(hdfsDAO.getJobResult("2"), timeout) should equal(None)
      Await.result(hdfsDAO.getJobResult("3"), timeout).get should equal(result)
    }

    ignore("should return false if delete is (partly) unsuccessful") {
      val result = "Result".toCharArray.map(_.toByte)
      Await.result(hdfsDAO.saveJobResult(testFileName, result), timeout) should equal(true)
      Await.result(hdfsDAO.deleteJobResults(Seq(testFileName, "nonexistent")), timeout) should equal(false)
      Await.result(hdfsDAO.getJobResult(testFileName), timeout) should equal(None)
    }

    ignore("should return true writing a file which already exists") {
      val testArray: Array[Byte] = "Test file".toCharArray.map(_.toByte)
      Await.result(hdfsDAO.saveJobResult(testFileName, testArray), timeout) should equal(true)
      Await.result(hdfsDAO.saveJobResult(testFileName, testArray), timeout) should equal(true)
    }
  }

  describe("check behavior if HDFS is misconfigured") {
    it("should return false if save is unsuccessful") {
      def config: Config = ConfigFactory.parseString(
        s"""spark.jobserver.binarydao.dir = "hdfs://foo:foo/binarydao-test""""
      )
      hdfsDAO = new HdfsBinaryObjectsDAO(config)
      Await.result(hdfsDAO.deleteBinary(testFileName), timeout) should equal(false)
    }
  }
}
