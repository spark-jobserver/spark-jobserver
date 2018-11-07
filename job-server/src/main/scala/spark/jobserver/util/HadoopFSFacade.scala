package spark.jobserver.util

import java.io.{InputStreamReader, Reader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/**
  * Facade to wrap hadoop filesystem operations.
  * If no parameter is passed then config file is expected to be on the classpath
  * otherwise inputs should be valid uri's with scheme.
  */
class HadoopFSFacade(hadoopConf: Configuration = new Configuration()) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * This function checks if file exists in any hadoop supported file system.
    * Example file path formats
    *   Local: /tmp/test.txt, file:/tmp/test.txt, file:///tmp/test.txt
    *   HDFS: hdfs:///tmp/test.txt, hdfs://namenode:port/tmp/test, s3:///tmp/test.txt
    * @param filePath path of jar file preferably with scheme
    * @return true if exists, false otherwise
    */
  def isFile(filePath: String): Option[Boolean] = {
    try {
      val fs = FileSystem.get(getPathURI(filePath), hadoopConf)
      Some(fs.isFile(new Path(filePath)))
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
  def get(filePath: String): Option[Reader] = {
    try {
      val fs = FileSystem.get(getPathURI(filePath), hadoopConf)
      val stream = fs.open(new Path(filePath))
      Some(new InputStreamReader(stream))
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
      val fs = FileSystem.get(path.toUri, hadoopConf)
      if (skipIfExists && fs.exists(path)) {
        logger.info(s"Skip saving the file: $filePath already exists on HDFS.")
      } else {
        val out = fs.create(path)
        Utils.usingResource(out) {
          out => out.write(bytes)
        }
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
      val fs = FileSystem.get(getPathURI(filePath), hadoopConf)
      fs.delete(new Path(filePath), recursive)
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage)
        false
    }
  }

  /**
    * This function returns a URI of the file path. If there is no scheme specified
    * it applies default scheme (defaults to "file:").
    * Example: /tmp/test.txt -> file:/tmp/test.txt
    *          hdfs:///tmp/test.txt -> hdfs:///tmp/test.txt (already has schema)
    * @param filePath path of the file
    * @return URI of the path
    */
  def getPathURI(filePath: String, defaultSchema: String = "file"): URI = {
    val path = new Path(filePath).toUri
    Option(path.getScheme) match {
      case None => new URI(s"$defaultSchema:$filePath")
      case _ => path
    }
  }
}

