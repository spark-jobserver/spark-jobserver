package spark.jobserver.io

import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.util.{HadoopFSFacade, Utils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Data access object for retrieving/persisting binary data on HDFS.
  * @param config config of jobserver
  */
class HdfsBinaryDAO(config: Config) extends BinaryDAO {
  if (!config.hasPath("spark.jobserver.binarydao.dir")) {
    throw new InvalidConfiguration(
      "To use HdfsBinaryDAO please specify absolute HDFS path in configuration file"
    )
  }

  private val logger = LoggerFactory.getLogger(getClass)

  private val binaryBasePath =
    config.getString("spark.jobserver.binarydao.dir").stripSuffix("/")
  private val hdfsFacade = new HadoopFSFacade()

  override def save(id: String, binaryBytes: Array[Byte]): Future[Boolean] = {
    Future {
      hdfsFacade.save(extendPath(id), binaryBytes, skipIfExists = true)
    }
  }

  override def delete(id: String): Future[Boolean] = {
    Future {
      hdfsFacade.delete(extendPath(id))
    }
  }

  override def get(id: String): Future[Option[Array[Byte]]] = {
    Future {
      hdfsFacade.get(extendPath(id)) match {
        case Some(inputStream) =>
          Some(Utils.usingResource(inputStream)(IOUtils.toByteArray))
        case None =>
          logger.error(s"Failed to get a file $id from HDFS.")
          None
      }
    }
  }

  private def extendPath(id: String): String = s"$binaryBasePath/$id"
}
