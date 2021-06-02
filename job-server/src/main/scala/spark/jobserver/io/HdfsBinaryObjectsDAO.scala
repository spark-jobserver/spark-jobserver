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
class HdfsBinaryObjectsDAO(config: Config) extends BinaryObjectsDAO {
  if (!config.hasPath("spark.jobserver.binarydao.dir")) {
    throw new InvalidConfiguration(
      "To use HdfsBinaryDAO please specify absolute HDFS path in configuration file"
    )
  }

  private val logger = LoggerFactory.getLogger(getClass)

  private val binaryBasePath =
    config.getString("spark.jobserver.binarydao.dir").stripSuffix("/")
  private val hdfsFacade = new HadoopFSFacade()

  override def saveBinary(id: String, binaryBytes: Array[Byte]): Future[Boolean] = {
    Future {
      hdfsFacade.save(getBinariesPath(id), binaryBytes, skipIfExists = true)
    }
  }

  override def deleteBinary(id: String): Future[Boolean] = {
    Future {
      hdfsFacade.delete(getBinariesPath(id))
    }
  }

  override def getBinary(id: String): Future[Option[Array[Byte]]] = {
    Future {
      hdfsFacade.get(getBinariesPath(id)) match {
        case Some(inputStream) =>
          Some(Utils.usingResource(inputStream)(IOUtils.toByteArray))
        case None =>
          logger.error(s"Failed to get a file $id from HDFS.")
          None
      }
    }
  }

  private def extendPath(id: String): String = s"$binaryBasePath/$id"

  override def saveJobResult(jobId: String, binaryBytes: Array[Byte]): Future[Boolean] = {
    Future {
      hdfsFacade.save(getJobResultsPath(jobId), binaryBytes, skipIfExists = true)
    }
  }

  override def getJobResult(jobId: String): Future[Option[Array[Byte]]] = {
    Future {
      hdfsFacade.get(getJobResultsPath(jobId)) match {
        case Some(inputStream) =>
          Some(Utils.usingResource(inputStream)(IOUtils.toByteArray))
        case None =>
          logger.error(s"Failed to get a job result file for job $jobId from HDFS.")
          None
      }
    }
  }

  override def deleteJobResults(jobIds: Seq[String]): Future[Boolean] = {
    Future {
      var success = true
      jobIds.map(getJobResultsPath(_))
        .foreach(path => {
          success &= hdfsFacade.delete(path)
        })
      success
    }
  }

  private def getBinariesPath(id: String): String = s"$binaryBasePath/$id"
  private def getJobResultsPath(id: String): String = s"$binaryBasePath/job_results/$id"
}
