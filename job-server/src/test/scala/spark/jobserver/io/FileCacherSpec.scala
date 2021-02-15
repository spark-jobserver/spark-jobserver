package spark.jobserver.io

import java.time.ZonedDateTime
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

class FileCacherSpec extends FileCacher with AnyFunSpecLike with Matchers {

  override val rootDir: Path = Paths.get(".")

  it("produces binary name") {
    val appName = createBinaryName("job", BinaryType.Jar, ZonedDateTime.parse("2016-10-10T13:00:00Z"))
    appName should be("job-20161010_130000_000.jar")
  }

  it("clean cache binaries") {
    val f = Files.createTempFile(rootDir, "jobTest-20161010_010000_000.jar", ".jar")
    cleanCacheBinaries("jobTest")
    Files.exists(f) should be(false)
  }

  it("should cache binary in current directory (default '.')") {
    val bytes = "some test content".toCharArray.map(_.toByte)
    val appName = "test-file-cached"
    val currentTime = ZonedDateTime.now
    val targetBinName = createBinaryName(appName, BinaryType.Jar, currentTime)
    cacheBinary(appName, BinaryType.Jar, currentTime, bytes)
    val file = rootDir.resolve(targetBinName)
    Files.exists(file) should be(true)
    cleanCacheBinaries(appName)
    Files.exists(file) should be(false)
  }

  it("should cache binary with special characters in current directory (default '.')") {
    val bytes = "some test content".toCharArray.map(_.toByte)
    val appName = "../test-file-cached"
    val currentTime = ZonedDateTime.now
    val targetBinName = createBinaryName(appName, BinaryType.Jar, currentTime)
    cacheBinary(appName, BinaryType.Jar, currentTime, bytes)
    val file = rootDir.resolve(targetBinName)
    Files.exists(file) should be(true)
    cleanCacheBinaries(appName)
    Files.exists(file) should be(false)
  }
}
