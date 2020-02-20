package spark.jobserver.io

import java.io.File

import org.joda.time.DateTime
import org.scalatest.{FunSpecLike, Matchers}

class FileCacherRandomDirSpec extends FileCacher with FunSpecLike with Matchers {
  override val rootDirPath: String = s"/tmp/spark-jobserver/${util.Random.alphanumeric.take(30).mkString}"
  override val rootDirFile: File = new File(rootDirPath)

  it("should create cache directory if it doesn't exist") {
    val bytes = "some test content".toCharArray.map(_.toByte)
    val appName = "test-file-cached"
    val currentTime = DateTime.now
    val targetBinName = createBinaryName(appName, BinaryType.Jar, currentTime)
    cacheBinary(appName, BinaryType.Jar, currentTime, bytes)
    val file = new File(rootDirPath, targetBinName)
    file.exists() should be(true)
    cleanCacheBinaries(appName)
    file.exists() should be(false)
  }
}
