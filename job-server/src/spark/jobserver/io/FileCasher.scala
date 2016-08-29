package spark.jobserver.io

import java.io.{BufferedOutputStream, File, FileOutputStream}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

trait FileCasher {

  val rootDir: String
  val rootDirFile: File

  private val logger = LoggerFactory.getLogger(getClass)

  def initFileDirectory(): Unit = {
    if (!rootDirFile.exists()) {
      if (!rootDirFile.mkdirs()) {
        throw new RuntimeException("Could not create directory " + rootDir)
      }
    }
  }

  def createBinaryName(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    appName + "-" + uploadTime.toString("yyyyMMdd_hhmmss_SSS") + s".${binaryType.extension}"
  }

  // Cache the jar file into local file system.
  protected def cacheBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          binBytes: Array[Byte]) {
    val outFile =
      new File(rootDir, createBinaryName(appName, binaryType, uploadTime))
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", binBytes.length, outFile.getPath)
      bos.write(binBytes)
      bos.flush()
    } finally {
      bos.close()
    }
  }

}
