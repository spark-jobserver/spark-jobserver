package spark.jobserver.io

import java.nio.file.{Files, Path, Paths}
import spark.jobserver.util.Utils

import java.time.ZonedDateTime
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class FileCacherRandomDirSpec extends FileCacher with AnyFunSpecLike with Matchers {

  private val parent = Paths.get("/tmp/spark-jobserver/")
  Utils.createDirectory(parent)
  override val rootDir: Path = Files.createTempDirectory(parent, "")

  it("should create cache directory if it doesn't exist") {
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
}
