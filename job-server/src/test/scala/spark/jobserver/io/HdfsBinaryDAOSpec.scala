package spark.jobserver.io

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import spark.jobserver.util.HDFSClusterLike

import scala.concurrent.Await
import scala.concurrent.duration._

class HdfsBinaryDAOSpec extends FunSpec with Matchers with BeforeAndAfterAll with HDFSClusterLike {
  private var hdfsDAO: HdfsBinaryDAO = _

  override def beforeAll: Unit = {
    super.startHDFS()
    val testClusterUrl = getNameNodeURI()
    def config: Config = ConfigFactory.parseString(
      s"""spark.jobserver.combineddao.binarydao.dir = "$testClusterUrl/binarydao-test""""
    )
    hdfsDAO = new HdfsBinaryDAO(config)
  }

  override def afterAll(): Unit = {
    hdfsDAO.delete(s"/") // cleanup artifacts
    super.shutdownHDFS()
  }

  describe("write, get, delete binary data") {
    it("should write, get, delete the file from hdfs") {
      val testArray: Array[Byte] = Array(1, 4, 4, 1)
      Await.result(hdfsDAO.get("test_file"), 60 seconds) should equal (None) // ensure there is no file before
      Await.result(hdfsDAO.save("test_file", testArray), 60 seconds) should equal (true)
      Await.result(hdfsDAO.get("test_file"), 60 seconds).get should equal (testArray)
      Await.result(hdfsDAO.delete("test_file"), 60 seconds) should equal (true)
      Await.result(hdfsDAO.get("test_file"), 60 seconds) should equal (None) // ensure there is no file after
    }

    it("should return nothing if file doesn't exist") {
      Await.result(hdfsDAO.get("some_random_file"), 60 seconds) should equal (None)
    }

    it("should return false if delete is unsuccessful") {
      Await.result(hdfsDAO.delete("some_random_file"), 60 seconds) should equal (false)
    }

    it("should return true writing a file which already exists") {
      val testArray: Array[Byte] = "Test file".toCharArray.map(_.toByte)
      Await.result(hdfsDAO.save("test_file", testArray), 60 seconds) should equal (true)
      Await.result(hdfsDAO.save("test_file", testArray), 60 seconds) should equal (true)
    }
  }

  describe("check behavior if HDFS is misconfigured") {
    it("should return false if save is unsuccessful") {
      def config: Config = ConfigFactory.parseString(
        s"""spark.jobserver.combineddao.binarydao.dir = "hdfs://foo:foo/binarydao-test""""
      )
      hdfsDAO = new HdfsBinaryDAO(config)
      Await.result(hdfsDAO.delete("some_random_file"), 60 seconds) should equal(false)
    }
  }
}
