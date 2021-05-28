package spark.jobserver.io

import java.io.File
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spark.jobserver.util._

import java.time.ZonedDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class JobDAOActorHelper(metaDataDAO: MetaDataDAO, binaryDAO: BinaryDAO, config: Config) extends FileCacher {
  import spark.jobserver.util.DAOMetrics._


  // Required by FileCacher
  val rootDirPath: String = config.getString(JobserverConfig.DAO_ROOT_DIR_PATH)
  val rootDirFile: File = new File(rootDirPath)

  implicit val daoTimeout = JobserverTimeouts.DAO_DEFAULT_TIMEOUT
  private val defaultAwaitTime = JobserverTimeouts.DAO_DEFAULT_TIMEOUT
  private val logger = LoggerFactory.getLogger(getClass)

  def saveBinary(name: String, binaryType: BinaryType, uploadTime: ZonedDateTime,
                          binaryBytes: Array[Byte]): Unit = {
    Utils.usingTimer(binWrite){ () =>
      val binHash = BinaryDAO.calculateBinaryHashString(binaryBytes)

      // saveBinary function is called from WebApi where as getBinaryFilePath is
      // called from driver whose location changes based on client/cluster mode.
      // Cache-on-upload feature is useful only for client mode because the drivers are
      // running on the same machine as jobserver.
      val cacheOnUploadEnabled = config.getBoolean("spark.jobserver.cache-on-upload")
      if (cacheOnUploadEnabled) {
        // The order is important. Save the jar file first and then log it into database.
        cacheBinary(name, binaryType, uploadTime, binaryBytes)
      }

      if (Await.result(binaryDAO.save(binHash, binaryBytes), defaultAwaitTime)) {
        if (Await.result(metaDataDAO.saveBinary(name, binaryType, uploadTime, binHash), defaultAwaitTime)) {
          totalSuccessfulSaveRequests.inc()
          logger.info(s"Successfully uploaded binary for $name")
        } else {
          totalFailedSaveMetadataDAORequests.inc()
          logger.error(s"Failed to save binary meta for $name, will try to delete file")
          metaDataDAO.getBinariesByStorageId(binHash).map(
            binaryInfos => binaryInfos.nonEmpty
          ) onComplete {
            case Success(false) => binaryDAO.delete(binHash) onComplete {
              case Success(true) => logger.info(s"Successfully deleted binary for $name after failed save.")
              case _ => logger.error(s"Failed to cleanup binary for $name after failed save.")
            }
            case _ => logger.info(s"Performing no cleanup, $name binary is used in meta data.")
          }
          throw SaveBinaryException(name)
        }
      } else {
        totalFailedSaveBinaryDAORequests.inc()
        logger.error(s"Failed to save binary data for $name, not proceeding with meta")
        throw SaveBinaryException(name)
      }
    }
  }

  def deleteBinary(name: String): Unit = {
    Utils.usingTimer(binDelete){ () =>
      Await.result(metaDataDAO.getBinary(name), defaultAwaitTime) match {
        case Some(binaryInfo) =>
          binaryInfo.binaryStorageId match {
            case Some(hash) =>
              if (Await.result(metaDataDAO.deleteBinary(name), defaultAwaitTime)) {
                val binInfosForHash = Await.result(metaDataDAO.getBinariesByStorageId(hash), defaultAwaitTime)
                if (binInfosForHash.exists(_.appName != binaryInfo.appName)) {
                  logger.warn(s"The binary '$name' is also uploaded under a different name. "
                    + s"The metadata for $name is deleted, but the binary is kept in the binary storage.")
                  totalSuccessfulDeleteRequests.inc()
                } else {
                  binaryDAO.delete(hash).map {_ =>
                    totalSuccessfulDeleteRequests.inc()
                  } onComplete {
                    case Failure(_) =>
                      totalFailedDeleteBinaryDAORequests.inc()
                      logger.error(s"Failed to delete binary file for $name, leaving an artifact")
                    case Success(_) =>
                  }
                }
                cleanCacheBinaries(name)
              }
              else {
                totalFailedDeleteMetadataDAORequests.inc()
                logger.error(s"Failed to delete binary meta for $name, not proceeding with file")
                throw DeleteBinaryInfoFailedException(name)
              }
            case _ =>
              totalFailedDeleteMetadataDAORequests.inc()
              logger.error(s"Failed to delete binary meta for $name, hash is not found")
              throw NoStorageIdException(name)
          }
        case None =>
          totalFailedDeleteMetadataDAORequests.inc()
          logger.warn(s"Couldn't find meta data information for $name")
          throw NoSuchBinaryException(name)
      }
    }
  }

  def getBinaryPath(name: String, binaryType: BinaryType, uploadTime: ZonedDateTime): String = {
    Utils.usingTimer(binRead){ () =>
      Await.result(Utils.usingTimer(binRead){ () => metaDataDAO.getBinary(name)}, defaultAwaitTime) match {
        case Some(binaryInfo) =>
          val binFile = new File(rootDirPath, createBinaryName(name, binaryType, uploadTime))
          binaryInfo.binaryStorageId match {
            case Some(_) =>
              if (!binFile.exists()) {
                Await.result(binaryDAO.get(binaryInfo.binaryStorageId.get), defaultAwaitTime) match {
                  case Some(binBytes) =>
                    cacheBinary(name, binaryType, uploadTime, binBytes)
                    binFile.getAbsolutePath
                  case None =>
                    logger.warn(s"Failed to fetch bytes from binary dao for $name/$uploadTime")
                    ""
                }
              } else {
                binFile.getAbsolutePath
              }
            case _ =>
              logger.error(s"Failed to get binary file path for $name, hash is not found")
              ""
          }
        case None =>
          logger.warn(s"Couldn't find meta data information for $name")
          ""
      }
    }
  }
}
