package spark.jobserver.io

import java.io.File

import java.time.ZonedDateTime
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class FileCacherRandomDirSpec extends FileCacher with AnyFunSpecLike with Matchers {
  override val rootDirPath: String = s"/tmp/spark-jobserver/${util.Random.alphanumeric.take(30).mkString}"
  override val rootDirFile: File = new File(rootDirPath)

  it("should create cache directory if it doesn't exist") {
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
