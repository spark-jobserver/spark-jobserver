package spark.jobserver

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.NoSuchElementException

import com.typesafe.config.Config
import org.joda.time.DateTime
import spark.jobserver.io._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
/**
  * In-memory DAO for easy unit testing
  */
class InMemoryDAO extends JobDAO {
  private val binaryDAO = new InMemoryBinaryDAO()
  private val metaDataDAO = new InMemoryMetaDAO()
  private val defaultTimeout = 3.seconds

  override def saveBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          binaryBytes: Array[Byte]): Unit = {
    val binHash = BinaryDAO.calculateBinaryHashString(binaryBytes)
    val saveFuture = for {
      _ <- binaryDAO.save(binHash, binaryBytes)
      result <- metaDataDAO.saveBinary(appName, binaryType, uploadTime, binHash)
    } yield result

    Await.result(saveFuture, defaultTimeout)
  }

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = {
    metaDataDAO.getBinaries.map(
      binaryInfos => binaryInfos.map(info => info.appName -> (info.binaryType, info.uploadTime)).toMap
    )
  }

  override def getBinaryFilePath(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    // Write the jar bytes to a temporary file
    val outFile = java.io.File.createTempFile("InMemoryDAO", s".${binaryType.extension}")
    outFile.deleteOnExit()
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      val binaryBytes = metaDataDAO.getBinary(appName)
        .flatMap(bin => binaryDAO.get(bin.get.binaryStorageId.get))
      bos.write(Await.result(binaryBytes, defaultTimeout).get)
      outFile.getAbsolutePath
    } catch {
      case _: NoSuchElementException => "" // DAOs return empty string if binary not found
    } finally {
      bos.close()
    }
  }

  override def deleteBinary(appName: String): Unit = {
    val future = for {
      binaryInfo <- metaDataDAO.getBinary(appName)
      _ <- metaDataDAO.deleteBinary(appName)
      result <- binaryDAO.delete(binaryInfo.get.binaryStorageId.get)
    } yield result

    Await.result(future, defaultTimeout)
  }

  override def saveContextInfo(contextInfo: ContextInfo): Unit = {
    Await.result(metaDataDAO.saveContext(contextInfo), defaultTimeout)
  }

  override def getContextInfo(id: String): Future[Option[ContextInfo]] =
    metaDataDAO.getContext(id)

  override def getContextInfos(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
        Future[Seq[ContextInfo]] =
    metaDataDAO.getContexts(limit, statuses)

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] =
    metaDataDAO.getContextByName(name)

  override def saveJobInfo(jobInfo: JobInfo): Unit =
    Await.result(metaDataDAO.saveJob(jobInfo), defaultTimeout)

  override def getJobInfos(limit: Int, statusOpt: Option[String] = None): Future[Seq[JobInfo]] =
    metaDataDAO.getJobs(limit, statusOpt)

  override def getJobInfosByContextId(contextId: String, jobStatuses: Option[Seq[String]] = None):
      Future[Seq[JobInfo]] = metaDataDAO.getJobsByContextId(contextId, jobStatuses)

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = metaDataDAO.getJob(jobId)

  override def saveJobConfig(jobId: String, jobConfig: Config): Unit =
    Await.result(metaDataDAO.saveJobConfig(jobId, jobConfig), defaultTimeout)

  override  def getJobConfig(jobId: String): Future[Option[Config]] = metaDataDAO.getJobConfig(jobId)

  override def getBinaryInfo(appName: String): Option[BinaryInfo] = {
    Await.result(metaDataDAO.getBinary(appName), defaultTimeout)
  }

  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
      Future[Seq[JobInfo]] = {
    throw new NotImplementedError()
  }
}
