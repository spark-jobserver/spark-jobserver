package spark.jobserver.io

import java.io.File

import com.typesafe.config.Config
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer.InvalidConfiguration

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Success

/**
  * @param config config of jobserver
  */
class CombinedDAO(config: Config) extends JobDAO with FileCacher {
  private val logger = LoggerFactory.getLogger(getClass)
  var binaryDAO: BinaryDAO = _
  var metaDataDAO: MetaDataDAO = _
  private val binaryDaoPath = "spark.jobserver.combineddao.binarydao.class"
  private val metaDataDaoPath = "spark.jobserver.combineddao.metadatadao.class"
  private val rootDirPath = "spark.jobserver.combineddao.rootdir"
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

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] =
    metaDataDAO.getBinaries.map(
      binaryInfos => binaryInfos.map(info => info.appName -> (info.binaryType, info.uploadTime)).toMap
    )

  override def saveContextInfo(contextInfo: ContextInfo): Unit = metaDataDAO.saveContext(contextInfo)

  override def getContextInfo(id: String): Future[Option[ContextInfo]] = metaDataDAO.getContext(id)

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = {
    metaDataDAO.getContextByName(name)
  }

  override def getContextInfos(limit: Option[Int],
                               statuses: Option[Seq[String]]): Future[Seq[ContextInfo]] = {
    metaDataDAO.getContexts(limit, statuses)
  }

  override def saveJobInfo(jobInfo: JobInfo): Unit = metaDataDAO.saveJob(jobInfo)

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = metaDataDAO.getJob(jobId)

  override def getJobInfos(limit: Int, status: Option[String]): Future[Seq[JobInfo]] = {
    metaDataDAO.getJobs(limit, status)
  }

  override def getJobInfosByContextId(contextId: String,
                                      jobStatuses: Option[Seq[String]]): Future[Seq[JobInfo]] = {
    metaDataDAO.getJobsByContextId(contextId, jobStatuses)
  }

  override def saveJobConfig(jobId: String, jobConfig: Config): Unit = {
    metaDataDAO.saveJobConfig(jobId, jobConfig)
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = {
    metaDataDAO.getJobConfig(jobId)
  }

  override def getLastUploadTimeAndType(name: String): Option[(DateTime, BinaryType)] = {
    val binaryInfo = Await.result(metaDataDAO.getBinary(name), defaultAwaitTime).getOrElse(return None)
    Some((binaryInfo.uploadTime, binaryInfo.binaryType))
  }

  override def saveBinary(name: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          binaryBytes: Array[Byte]): Unit = {
    val binHash = BinaryDAO.calculateBinaryHashString(binaryBytes)
    val isBinarySaved = binaryDAO.save(binHash, binaryBytes)
    isBinarySaved onComplete {
      case Success(true) =>
        metaDataDAO.saveBinary(name, binaryType, uploadTime, binHash) onComplete {
          case Success(true) => logger.info(s"Successfully uploaded binary for $name")
          case _ =>
            logger.error(s"Failed to save binary meta for $name, will try to delete file")
            isBinaryUsed(binHash) onComplete {
              case Success(false) => binaryDAO.delete(binHash) onComplete {
                case Success(true) =>
                  logger.info(s"Successufully deleted binary for $name after failed save.")
                case _ => logger.error(s"Failed to cleanup binary for $name after failed save.")
              }
              case _ => logger.info(s"Perfoming no cleanup, $name binary is used in meta data.")
            }
        }
      case _ => logger.error(s"Failed to save binary data for $name, not proceeding with meta")
    }
  }

  override def deleteBinary(name: String): Unit = {
    val binaryMeta = metaDataDAO.getBinary(name)
    binaryMeta onComplete   {
      case Success(Some(binaryInfo)) =>
        metaDataDAO.deleteBinary(name) onComplete {
          case Success(true) =>
            isBinaryUsed(binaryInfo.binaryStorageId, binaryInfo.appName) onComplete {
              case Success(false) =>
                  binaryDAO.delete(binaryInfo.binaryStorageId) onFailure {
                  case _ => logger.error(s"Failed to delete binary file for $name, leaving an artifact")
                }
              case _ =>
                logger.error(s"$name binary is used by other applications, not deleting it from storage")
            }
          case _ => logger.error(s"Failed to delete binary meta for $name, not proceeding with file")
        }
      case _ => logger.error(s"Caught unexpected error try to get $name binary")
    }
  }

  override def getBinaryFilePath(name: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    Await.result(metaDataDAO.getBinary(name), defaultAwaitTime) match {
      case Some(binaryInfo) =>
        val binFile = new File(rootDir, createBinaryName(name, binaryType, uploadTime))
        if (!binFile.exists()) {
              val binBytes = Await.result(binaryDAO.get(binaryInfo.binaryStorageId), defaultAwaitTime)
              cacheBinary(name, binaryType, uploadTime, binBytes.getOrElse(return ""))
          }
        binFile.getAbsolutePath
      case _ => ""
    }
  }

  /**
    * Checks if binaryStorageId is referenced in any currently saved binary meta.
    * @param binaryStorageId id to check usage for
    * @param excludeName if given, will check refence under other name (won't count reference from this name)
    * @return true if there are existing references, else false
    */
  def isBinaryUsed(binaryStorageId: String, excludeName: String = ""): Future[Boolean] = {
    if (excludeName == "") {
      metaDataDAO.getBinaries.map(
        binaryInfos => binaryInfos.exists(_.binaryStorageId == binaryStorageId)
      )
    } else {
      metaDataDAO.getBinaries.map(
        binaryInfos => binaryInfos.exists(
          i => i.binaryStorageId == binaryStorageId && i.appName != excludeName
        )
      )
    }
  }
}
