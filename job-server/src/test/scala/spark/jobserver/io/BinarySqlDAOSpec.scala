package spark.jobserver.io

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfter
import spark.jobserver.TestJarFinder

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

abstract class BinarySqlDAOSpecBase {
  def config : Config
}

class BinarySqlDAOSpec extends  BinarySqlDAOSpecBase with TestJarFinder with AnyFunSpecLike with Matchers
  with BeforeAndAfter {
  override def config: Config = ConfigFactory.load("local.test.binarysqldao.conf")
  private val timeout = 60 seconds
  private var dao: SqlBinaryObjectsDAO = _

  private val testBlobName = "test_file"

  before {
    dao = new SqlBinaryObjectsDAO(config)
  }

  after {
    val profile = dao.dbUtils.profile
    import profile.api._

    Await.result(dao.dbUtils.db.run(dao.blobs.delete), timeout)
  }

  describe("write, get, delete binary data") {
    it("should write, get, delete the file from hdfs") {
      val validInvalidInput = "Ḽơᶉëᶆ ȋṕšᶙṁ � � � � test 1 4 4 1".toCharArray.map(_.toByte)
      Await.result(
        dao.getBinary(testBlobName), timeout) should equal(None)
      Await.result(dao.saveBinary(testBlobName, validInvalidInput), timeout) should equal(true)
      Await.result(dao.getBinary(testBlobName), timeout).get should equal(validInvalidInput)
      Await.result(dao.deleteBinary(testBlobName), timeout) should equal(true)
      Await.result(
        dao.getBinary(testBlobName), timeout) should equal(None)
    }

    it("should return nothing if file doesn't exist") {
      Await.result(dao.getBinary(testBlobName), timeout) should equal (None)
    }

    it("should return false if delete is unsuccessful") {
      Await.result(dao.deleteBinary(testBlobName), timeout) should equal (false)
    }

    it("should return true writing a file which already exists") {
      val testArray: Array[Byte] = "Test file".toCharArray.map(_.toByte)
      Await.result(dao.saveBinary(testBlobName, testArray), timeout) should equal (true)
      Await.result(dao.saveBinary(testBlobName, testArray), timeout) should equal (true)
    }
  }

  describe("write, get, delete job result binary data") {
    it("should write, get, delete the file from hdfs") {
      val validInvalidInput = "Ḽơᶉëᶆ ȋṕšᶙṁ � � � � test 1 4 4 1".toCharArray.map(_.toByte)
      Await.result(
        dao.getJobResult(testBlobName), timeout) should equal(None)
      Await.result(dao.saveJobResult(testBlobName, validInvalidInput), timeout) should equal(true)
      Await.result(dao.getJobResult(testBlobName), timeout).get should equal(validInvalidInput)
      Await.result(dao.deleteJobResult(testBlobName), timeout) should equal(true)
      Await.result(
        dao.getJobResult(testBlobName), timeout) should equal(None)
    }

    it("should return nothing if file doesn't exist") {
      Await.result(dao.getJobResult(testBlobName), timeout) should equal (None)
    }

    it("should return false if delete is unsuccessful") {
      Await.result(dao.deleteJobResult(testBlobName), timeout) should equal (false)
    }

    it("should return true writing a file which already exists") {
      val testArray: Array[Byte] = "Test file".toCharArray.map(_.toByte)
      Await.result(dao.saveJobResult(testBlobName, testArray), timeout) should equal (true)
      Await.result(dao.saveJobResult(testBlobName, testArray), timeout) should equal (true)
    }
  }
}