package spark.jobserver.util

import java.io.{IOException, InputStreamReader, Reader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

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
      val path = new Path(filePath)
      val uri = getURI(path)
      val fs = FileSystem.get(uri, hadoopConf)
      Some(fs.isFile(path))
    } catch {
      case e @ (_: IllegalArgumentException | _: IOException) =>
        logger.error(e.getMessage)
        None
    }
  }

  /**
    * This function can read config file from all the supported protocols by HDFS
    * @param filePath absolute path of config file with any supported protocol file/hdfs/s3
    * @return file stream
    */
  def get(filePath: String): Option[Reader] = {
    try {
      val fs = FileSystem.get(getURI(filePath), hadoopConf)
      val stream = fs.open(new Path(filePath))
      Some(new InputStreamReader(stream))
    } catch {
      case e @ (_: IllegalArgumentException | _ : IOException) =>
        logger.error(e.getMessage)
        None
    }
  }

  private def getURI(filePath: Path): URI = {
    Option(filePath.toUri().getScheme()) match {
      case None => new URI(s"file:${filePath.toString}")
      case _ => filePath.toUri()
    }
  }

  private def getURI(filePath: String): URI = {
    val path = new Path(filePath)
    getURI(path)
  }
}
