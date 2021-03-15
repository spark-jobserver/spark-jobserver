package spark.jobserver.io

import java.io.File
import org.scalatest.{FunSpecLike, Matchers}

import java.time.ZonedDateTime

class FileCacherSpec extends FileCacher with FunSpecLike with Matchers {

  override val rootDirPath: String = "."
  override val rootDirFile: File = new File(rootDirPath)

  it("produces binary name") {
    val appName = createBinaryName("job", BinaryType.Jar, ZonedDateTime.parse("2016-10-10T13:00:00Z"))
    appName should be("job-20161010_130000_000.jar")
  }

  it("clean cache binaries") {
    val f = File.createTempFile("jobTest-20161010_010000_000.jar", ".jar", new File(rootDirPath))
    cleanCacheBinaries("jobTest")
    f.exists() should be(false)
  }

  it("should cache binary in current directory (default '.')") {
    val bytes = "some test content".toCharArray.map(_.toByte)
    val appName = "test-file-cached"
    val currentTime = ZonedDateTime.now
    val targetBinName = createBinaryName(appName, BinaryType.Jar, currentTime)
    cacheBinary(appName, BinaryType.Jar, currentTime, bytes)
    val file = new File(rootDirPath, targetBinName)
    file.exists() should be(true)
    cleanCacheBinaries(appName)
    file.exists() should be(false)
  }
}
