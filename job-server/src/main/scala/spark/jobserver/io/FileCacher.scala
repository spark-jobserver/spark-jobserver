package spark.jobserver.io

import java.io.{BufferedOutputStream, File, FileOutputStream, FilenameFilter}
import org.slf4j.LoggerFactory
import spark.jobserver.util.Utils

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

trait FileCacher {

  val rootDirPath: String
  val rootDirFile: File

  private val logger = LoggerFactory.getLogger(getClass)
  private val df = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")


  // date format
  val Pattern = "\\d{8}_\\d{6}_\\d{3}".r

  def createBinaryName(appName: String, binaryType: BinaryType, uploadTime: ZonedDateTime): String = {
    appName + "-" + df.format(uploadTime) + s".${binaryType.extension}"
  }

  protected def getPath(appName: String, binaryType: BinaryType,
                        uploadTime: ZonedDateTime): Option[String] = {
    val binFile = new File(rootDirPath, createBinaryName(appName, binaryType, uploadTime))
    if(binFile.exists()) {
      Some(binFile.getAbsolutePath)
    } else {
      None
    }
  }

  // Cache the binary file into local file system.
  protected def cacheBinary(appName: String,
                            binaryType: BinaryType,
                            uploadTime: ZonedDateTime,
                            binBytes: Array[Byte]): String = {
    val targetFullBinaryName = createBinaryName(appName, binaryType, uploadTime)
    val tempSuffix = ".tmp"
    val rootDir = new File(rootDirPath)
    Utils.createDirectory(rootDir)
    val tempOutFile = File.createTempFile(targetFullBinaryName + "-", tempSuffix, rootDir)
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

    val tempFile = new File(rootDirPath, tempOutFileName)
    val renamedFile = new File(rootDirPath, targetFullBinaryName)
    renamedFile.deleteOnExit()
    if (!tempFile.renameTo(renamedFile)) {
      logger.debug("Renaming the temporary file {} failed, another process has probably already updated " +
        "the target file - deleting the redundant temp file", tempOutFileName)
      if (!tempFile.delete()) {
        logger.warn("Could not delete the temporary file {}", tempOutFileName)
      }
    }

    renamedFile.getAbsolutePath
  }

  protected def cleanCacheBinaries(appName: String): Unit = {
    val dir = new File(rootDirPath)
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
