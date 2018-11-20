package spark.jobserver.util

import java.io.{BufferedReader, File, Reader}

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

class HadoopFSFacadeSpec extends FunSpec with Matchers with BeforeAndAfterAll with HDFSClusterLike {
  private var config: Config = _
  private var testClusterUrl: String = _
  private var hdfsFacade: HadoopFSFacade = _
  private val testFilePath = "/tmp/hdfsfacadetest"

  override def beforeAll: Unit = {
    super.startHDFS()
    testClusterUrl = getNameNodeURI()
    hdfsFacade = new HadoopFSFacade()
  }

  override def afterAll(): Unit = {
    hdfsFacade.delete(testFilePath)  // cleanup artifacts
    super.shutdownHDFS()
  }

  describe("write, delete and save the data") {
    it("should write, get and delete the file without schema (local FS)") {
      new File(testFilePath).exists() should equal(false)
      hdfsFacade.save(testFilePath, "test".toCharArray.map(_.toByte))
      readerToString(hdfsFacade.get(testFilePath).get) should equal("test")
      new File(testFilePath).exists() should equal(true)
      hdfsFacade.delete(testFilePath) should equal(true)
      hdfsFacade.get(testFilePath) should equal(None)
    }

    it("should write, get and delete the file using defaultFS (HDFS)") {
      val config = new Configuration()
      config.set("fs.defaultFS", testClusterUrl)
      hdfsFacade = new HadoopFSFacade(config)
      hdfsFacade.save(testFilePath, "test".toCharArray.map(_.toByte))
      readerToString(hdfsFacade.get(testFilePath).get) should equal("test")
      // check operations with full uri as well, to be sure that it's hdfs, not local system
      readerToString(hdfsFacade.get(s"$testClusterUrl/$testFilePath").get) should equal("test")
      hdfsFacade.delete(testFilePath) should equal(true)
      hdfsFacade.delete(s"$testClusterUrl/$testFilePath") should equal(false)
      hdfsFacade.get(testFilePath) should equal(None)
      hdfsFacade.get(s"$testClusterUrl/$testFilePath") should equal(None)
    }

    it("should overwrite default scheme if specified") {
      val config = new Configuration()
      config.set("fs.defaultFS", "file:///")
      hdfsFacade = new HadoopFSFacade(config, defaultFS = testClusterUrl)
      hdfsFacade.get(s"$testClusterUrl/$testFilePath") should equal(None)
      hdfsFacade.save(testFilePath, "test".toCharArray.map(_.toByte))
      readerToString(hdfsFacade.get(testFilePath).get) should equal("test")
      readerToString(hdfsFacade.get(s"$testClusterUrl/$testFilePath").get) should equal("test")
      hdfsFacade.delete(testFilePath) should equal(true)
    }

    it("should not overwrite default schema if it is used") {
      val config = new Configuration()
      config.set("fs.defaultFS", testClusterUrl)
      hdfsFacade = new HadoopFSFacade(config, defaultFS = "file:///")
      hdfsFacade.save(s"hdfs://$testFilePath", "test".toCharArray.map(_.toByte))
      readerToString(hdfsFacade.get(s"hdfs://$testFilePath").get) should equal("test")
      readerToString(hdfsFacade.get(s"$testClusterUrl/$testFilePath").get) should equal("test")
      hdfsFacade.delete(s"hdfs://$testFilePath") should equal(true)
    }
  }

  describe("should check if file exists") {
    it("should return true if file exists") {
      hdfsFacade.save(testFilePath, "test".toCharArray.map(_.toByte))
      hdfsFacade.isFile(testFilePath) should equal(Some(true))
    }

    it("should return false if file doesn't exist") {
      hdfsFacade.isFile("some_not_existing_file") should equal(Some(false))
    }
  }

  private def readerToString(is: Reader): String = {
    val bufferedReader = new BufferedReader(is)
    Iterator.continually(bufferedReader.readLine).takeWhile(_ != null).mkString
  }
}
