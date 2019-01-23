package spark.jobserver.io

import java.io.File

import org.joda.time.DateTime
import org.scalatest.{FunSpecLike, Matchers}

class FileCacherSpec extends FileCacher with FunSpecLike with Matchers {

  override val rootDir: String = "."
  override val rootDirFile: File = new File(rootDir)

  it("produces binary name") {
    val appName = createBinaryName("job", BinaryType.Jar, DateTime.parse("2016-10-10T13:00:00Z"))
    appName should be("job-20161010_130000_000.jar")
  }

  it("clean cache binaries") {
    val f = File.createTempFile("jobTest-20161010_010000_000.jar", ".jar", new File(rootDir))
    cleanCacheBinaries("jobTest")
    f.exists() should be(false)
  }
}
