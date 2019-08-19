package spark.jobserver.io

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import slick.SlickException
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.util._
import spark.jobserver.io.CombinedDAO._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Success
import spark.jobserver.common.akka.metrics.YammerMetrics

object CombinedDAO {
  val binaryDaoPath = "spark.jobserver.combineddao.binarydao.class"
  val metaDataDaoPath = "spark.jobserver.combineddao.metadatadao.class"
  val rootDirPath = "spark.jobserver.combineddao.rootdir"
}

/**
  * @param config config of jobserver
  */
class CombinedDAO(config: Config) extends JobDAO with FileCacher with YammerMetrics {
  private val logger = LoggerFactory.getLogger(getClass)

  // Metrics
  private val saveTimerMs = timer("total-save-time", TimeUnit.MILLISECONDS)
  private val totalSuccessfulSaveRequests = counter("total-save-binary-success")
  private val totalFailedSaveBinaryDAORequests = counter("total-binary-save-binary-dao-failed")
  private val totalFailedSaveMetadataDAORequests = counter("total-binary-save-metadata-dao-failed")

  private val deleteTimerMs = timer("total-delete-time", TimeUnit.MILLISECONDS)
  private val totalSuccessfulDeleteRequests = counter("total-delete-binary-success")
  private val totalFailedDeleteBinaryDAORequests = counter("total-binary-delete-binary-dao-failed")
  private val totalFailedDeleteMetadataDAORequests = counter("total-binary-delete-metadata-dao-failed")

  var binaryDAO: BinaryDAO = _
  var metaDataDAO: MetaDataDAO = _
  if (!(config.hasPath(binaryDaoPath) && config.hasPath(metaDataDaoPath) && config.hasPath(rootDirPath))) {
    throw new InvalidConfiguration(
      "To use CombinedDAO root directory and BinaryDAO, MetaDataDAO classes should be specified"
    )
  }

  try {
    binaryDAO = Class.forName(config.getString(binaryDaoPath))
      .getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
      .newInstance(config).asInstanceOf[BinaryDAO]
    metaDataDAO = Class.forName(
      config.getString(metaDataDaoPath))
      .getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
      .newInstance(config).asInstanceOf[MetaDataDAO]
  } catch {
    case error: ClassNotFoundException =>
      logger.error(error.getMessage)
      throw new InvalidConfiguration(
      "Couldn't create Binary and Metadata DAO instances: please check configuration"
      )
  }

  val rootDir: String = config.getString(rootDirPath)
  val rootDirFile: File = new File(rootDir)

  private val defaultAwaitTime = 60 seconds

  initFileDirectory()

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] =
    metaDataDAO.getBinaries.map(
      binaryInfos => binaryInfos.map(info => info.appName -> (info.binaryType, info.uploadTime)).toMap
    )

  override def saveContextInfo(contextInfo: ContextInfo): Unit = {
    val isSaved = Await.result(metaDataDAO.saveContext(contextInfo), defaultAwaitTime)
    if(!isSaved) {
      throw new SlickException(s"Could not update ${contextInfo.id} in the database")
    }
  }

  override def getContextInfo(id: String): Future[Option[ContextInfo]] = metaDataDAO.getContext(id)

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = {
    metaDataDAO.getContextByName(name)
  }

  override def getContextInfos(limit: Option[Int],
                               statuses: Option[Seq[String]]): Future[Seq[ContextInfo]] = {
    metaDataDAO.getContexts(limit, statuses)
  }

  override def saveJobInfo(jobInfo: JobInfo): Unit = {
    val isSaved = Await.result(metaDataDAO.saveJob(jobInfo), defaultAwaitTime)
    if(!isSaved) {
      throw new SlickException(s"Could not update ${jobInfo.jobId} in the database")
    }
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = metaDataDAO.getJob(jobId)

  override def getJobInfos(limit: Int, status: Option[String]): Future[Seq[JobInfo]] = {
    metaDataDAO.getJobs(limit, status)
  }

  override def getJobInfosByContextId(contextId: String,
                                      jobStatuses: Option[Seq[String]]): Future[Seq[JobInfo]] = {
    metaDataDAO.getJobsByContextId(contextId, jobStatuses)
  }

  override def saveJobConfig(jobId: String, jobConfig: Config): Unit = {
    val isSaved = Await.result(metaDataDAO.saveJobConfig(jobId, jobConfig), defaultAwaitTime)
    if(!isSaved) {
      throw new SlickException(s"Could not save job config into database for $jobId")
    }
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = {
    metaDataDAO.getJobConfig(jobId)
  }

  override def getBinaryInfo(name: String): Option[BinaryInfo] = {
    val binaryInfo = Await.result(metaDataDAO.getBinary(name), defaultAwaitTime).getOrElse(return None)
    Some(binaryInfo)
  }

  override def saveBinary(name: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          binaryBytes: Array[Byte]): Unit = {
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

    val saveTimer = saveTimerMs.time()
    if (Await.result(binaryDAO.save(binHash, binaryBytes), defaultAwaitTime)) {
      if (Await.result(metaDataDAO.saveBinary(name, binaryType, uploadTime, binHash), defaultAwaitTime)) {
        saveTimer.stop()
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
        saveTimer.stop()
        throw SaveBinaryException(name)
      }
    } else {
      saveTimer.stop()
      totalFailedSaveBinaryDAORequests.inc()
      logger.error(s"Failed to save binary data for $name, not proceeding with meta")
      throw SaveBinaryException(name)
    }
  }

  override def deleteBinary(name: String): Unit = {
    val deleteTimer = deleteTimerMs.time()
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
                } onFailure {
                  case _ =>
                    totalFailedDeleteBinaryDAORequests.inc()
                    logger.error(s"Failed to delete binary file for $name, leaving an artifact")
                }
              }
              deleteTimer.stop()
              cleanCacheBinaries(name)
            }
            else {
              deleteTimer.stop()
              totalFailedDeleteMetadataDAORequests.inc()
              logger.error(s"Failed to delete binary meta for $name, not proceeding with file")
              throw DeleteBinaryInfoFailedException(name)
            }
          case _ =>
            deleteTimer.stop()
            totalFailedDeleteMetadataDAORequests.inc()
            logger.error(s"Failed to delete binary meta for $name, hash is not found")
            throw NoStorageIdException(name)
        }
      case None =>
        deleteTimer.stop()
        totalFailedDeleteMetadataDAORequests.inc()
        logger.warn(s"Couldn't find meta data information for $name")
        throw NoSuchBinaryException(name)
    }
  }

  override def getBinaryFilePath(name: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    Await.result(metaDataDAO.getBinary(name), defaultAwaitTime) match {
      case Some(binaryInfo) =>
        val binFile = new File(rootDir, createBinaryName(name, binaryType, uploadTime))
        binaryInfo.binaryStorageId match {
          case Some(_) =>
            if (!binFile.exists()) {
              Await.result(binaryDAO.get(binaryInfo.binaryStorageId.get), defaultAwaitTime) match {
                case Some(binBytes) => cacheBinary(name, binaryType, uploadTime, binBytes)
                case None =>
                  logger.warn(s"Failed to fetch bytes from binary dao for $name/$uploadTime")
                  return ""
              }
            }
            binFile.getAbsolutePath
          case _ =>
            logger.error(s"Failed to get binary file path for $name, hash is not found")
            ""
        }
      case None =>
        logger.warn(s"Couldn't find meta data information for $name")
        ""
    }
  }

  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
      Future[Seq[JobInfo]] = {
    metaDataDAO.getJobsByBinaryName(binName, statuses)
  }
}
