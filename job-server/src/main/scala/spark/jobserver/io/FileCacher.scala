package spark.jobserver.io

import java.io.{BufferedOutputStream, File, FileOutputStream, FilenameFilter}

import org.joda.time.DateTime
import org.slf4j.LoggerFactory

trait FileCacher {

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

  // date format
  val Pattern = "\\d{8}_\\d{6}_\\d{3}".r

  def createBinaryName(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    appName + "-" + uploadTime.toString("yyyyMMdd_HHmmss_SSS") + s".${binaryType.extension}"
  }

  // Cache the jar file into local file system.
  protected def cacheBinary(appName: String,
                            binaryType: BinaryType,
                            uploadTime: DateTime,
                            binBytes: Array[Byte]) {
    val targetFullBinaryName = createBinaryName(appName, binaryType, uploadTime)
    val tempSuffix = ".tmp"
    val tempOutFile = File.createTempFile(targetFullBinaryName + "-", tempSuffix, new File(rootDir))
    val tempOutFileName = tempOutFile.getName
    val bos = new BufferedOutputStream(new FileOutputStream(tempOutFile))

    try {
      logger.debug("Writing {} bytes to a temporary file {}", binBytes.length, tempOutFile.getPath)
      bos.write(binBytes)
      bos.flush()
    } finally {
      bos.close()
    }

    logger.debug("Renaming the temporary file {} to the target full binary name {}",
      tempOutFileName, targetFullBinaryName: Any)

    val tempFile = new File(rootDir, tempOutFileName)
    if (!tempFile.renameTo(new File(rootDir, targetFullBinaryName))) {
      logger.debug("Renaming the temporary file {} failed, another process has probably already updated " +
        "the target file - deleting the redundant temp file", tempOutFileName)
      if (!tempFile.delete()) {
        logger.warn("Could not delete the temporary file {}", tempOutFileName)
      }
    }
  }

  protected def cleanCacheBinaries(appName: String): Unit = {
    val dir = new File(rootDir)
    val binaries = dir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        val prefix = appName + "-"
        if (name.startsWith(prefix)) {
          val suffix = name.substring(prefix.length)
          (Pattern findFirstIn suffix).isDefined
        } else {
          false
        }
      }
    })
    if (binaries != null) {
      binaries.foreach(f => f.delete())
    }
  }

}
