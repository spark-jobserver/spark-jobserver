package spark.jobserver.util

import java.io.InputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/**
  * Facade to wrap hadoop filesystem operations.
  * If no parameter is passed then config file is expected to be on the classpath
  * otherwise inputs should be valid uri's with scheme.
  */
class HadoopFSFacade(hadoopConf: Configuration = new Configuration(),
                     implicit val defaultFS: String = "") {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * This function checks if file exists in any hadoop supported file system.
    * Example file path formats
    *   Local: file:/tmp/test.txt, file:///tmp/test.txt
    *   HDFS: hdfs:///tmp/test.txt, hdfs://namenode:port/tmp/test, s3:///tmp/test.txt
    * @param filePath path of jar file preferably with scheme
    * @return true if exists, false otherwise
    */
  def isFile(filePath: String): Option[Boolean] = {
    try {
      val path = new Path(filePath)
      val fs = getFileSystem(filePath)(defaultFS)
      Some(fs.isFile(path))
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage)
        None
    }
  }

  /**
    * This function can read file from all the supported protocols by HDFS
    * @param filePath absolute path of the file with any supported protocol file/hdfs/s3
    * @return file stream
    */
  def get(filePath: String): Option[InputStream] = {
    try {
      val path = new Path(filePath)
      val fs = getFileSystem(filePath)
      Some(fs.open(path))
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage)
        None
    }
  }

  /**
    * This function can save given bytes under the given file path
    * @param filePath file path where the file should be saved
    * @param bytes bytes to be saved under the given filePath
    * @param skipIfExists do nothing if file with given name already exists, return success
    * @return true if operation succeeds, else false
    */
  def save(filePath: String, bytes: Array[Byte], skipIfExists: Boolean = false): Boolean = {
    try {
      val path = new Path(filePath)
      val fs = getFileSystem(filePath)
      if (skipIfExists && fs.exists(path)) {
        logger.info(s"Skip saving the file: $filePath already exists on HDFS.")
      } else {
        val out = fs.create(path)
        Utils.usingResource(out) {
          out => out.write(bytes)
        }
        logger.info(s"Saved the binary data for $filePath.")
      }
      true
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage)
        false
    }
  }

  /**
    * This function can delete file(s) under the given file path
    * @param filePath path of the file to be deleted
    * @param recursive if set to true will delete all subfolders
    * @return true if operation succeeds, else false
    */
  def delete(filePath: String, recursive: Boolean = false): Boolean = {
    try {
      val path = new Path(filePath)
      val fs = getFileSystem(filePath)
      logger.info(s"Deleting file $filePath.")
      fs.delete(path, recursive)
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage)
        false
    }
  }

  /**
    * Creates file system object taking in account preferred defaultFS. If path doesn't use current defaultFS
    * settings, overwrite to wished value.
    * Examples:
    *   path: /path/to/file, defaultFS: hdfs://localhost:8020, wished defaultFS: file:/// -> overwrite
    *   path: hdfs:///path, defaultFS: hdfs://localhost:8020, wished defaultFS: file:/// -> keep defaults
    * This functions helps HadoopFS to act as local FS for configuration files
    * @param filePath path for which to create file system object
    * @param defaultFS (implicit) new value for defaultFS. if not needed, pass empty string.
    * @return file system object, with needed defaultFS depending on given path and real defaultFS value
    */
  def getFileSystem(filePath: String)(implicit defaultFS: String): FileSystem = {
    if (defaultFS != "") {
      val defaultFSPrefix = hadoopConf.get("fs.defaultFS", "").split(":")(0)
      // don't overwrite default schema if it's used
      if (defaultFSPrefix != "" && !filePath.startsWith(defaultFSPrefix)) {
        val modifiedConf = new Configuration(hadoopConf)
        modifiedConf.set("fs.defaultFS", defaultFS)
        return FileSystem.get(new Path(filePath).toUri, modifiedConf)
      }
    }
    FileSystem.get(new Path(filePath).toUri, hadoopConf)
  }
}

