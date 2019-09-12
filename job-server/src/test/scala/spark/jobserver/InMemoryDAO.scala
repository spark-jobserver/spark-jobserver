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
  var binaries = mutable.HashMap.empty[(String, BinaryType, DateTime), (Array[Byte])]

  override def saveBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          binaryBytes: Array[Byte]): Unit = {
    binaries((appName, binaryType, uploadTime)) = binaryBytes
  }

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = {
    Future {
      binaries.keys
        .groupBy(_._1)
        .map { case (appName, appUploadTimeTuples) =>
          appName -> appUploadTimeTuples.map(t => (t._2, t._3)).toSeq.head
        }
    }
  }

  override def getBinaryFilePath(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    // Write the jar bytes to a temporary file
    val outFile = java.io.File.createTempFile("InMemoryDAO", s".${binaryType.extension}")
    outFile.deleteOnExit()
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      bos.write(binaries((appName, binaryType, uploadTime)))
      outFile.getAbsolutePath
    } catch {
      case e: NoSuchElementException => "" // DAOs return empty string if binary not found
    } finally {
      bos.close()
    }
  }

  val contextInfos = mutable.HashMap.empty[String, ContextInfo]

  override def saveContextInfo(contextInfo: ContextInfo): Unit = {
    contextInfos(contextInfo.id) = contextInfo
  }

  override def getContextInfo(id: String): Future[Option[ContextInfo]] = Future {
    contextInfos.get(id)
  }

  private def sortTime(c1: ContextInfo, c2: ContextInfo): Boolean = {
      // If both dates are the same then it will return false
      c1.startTime.isAfter(c2.startTime)
  }

  override def getContextInfos(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
        Future[Seq[ContextInfo]] = Future {
    val allContexts = contextInfos.values.toSeq.sortWith(sortTime)
    val filteredContexts = statuses match {
      case Some(statuses) =>
        allContexts.filter(j => statuses.contains(j.state))
      case _ => allContexts
    }

    limit match {
      case Some(l) => filteredContexts.take(l)
      case _ => filteredContexts
    }
  }

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = Future {
    contextInfos.values.toSeq.sortWith(sortTime).filter(_.name == name).headOption
  }

  val jobInfos = mutable.HashMap.empty[String, JobInfo]

  override def saveJobInfo(jobInfo: JobInfo) { jobInfos(jobInfo.jobId) = jobInfo }

  def getJobInfos(limit: Int, statusOpt: Option[String] = None): Future[Seq[JobInfo]] = Future {
    val allJobs = jobInfos.values.toSeq.sortBy(_.startTime.toString())
    val filterJobs = statusOpt match {
      case Some(JobStatus.Running) => {
        allJobs.filter(jobInfo => jobInfo.endTime.isEmpty && jobInfo.error.isEmpty)
      }
      case Some(JobStatus.Error) => allJobs.filter(_.error.isDefined)
      case Some(JobStatus.Finished) => {
        allJobs.filter(jobInfo => jobInfo.endTime.isDefined && jobInfo.error.isEmpty)
      }
      case _ => allJobs
    }
    filterJobs.take(limit)
  }

  override def getJobInfosByContextId(
      contextId: String, jobStatuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = Future {
    jobInfos.values.toSeq.filter(j => {
      (contextId, jobStatuses) match {
        case (contextId, Some(statuses)) => contextId == j.contextId && statuses.contains(j.state)
        case _ => contextId == j.contextId
      }
    })
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = Future {
    jobInfos.get(jobId)
  }

  val jobConfigs = mutable.HashMap.empty[String, Config]

  override def saveJobConfig(jobId: String, jobConfig: Config) { jobConfigs(jobId) = jobConfig }

  override  def getJobConfig(jobId: String): Future[Option[Config]] = Future {
    jobConfigs.get(jobId)
  }

  override def getBinaryInfo(appName: String): Option[BinaryInfo] = {
    // Copied from the base JobDAO, feel free to optimize this (having in mind this specific storage type)
    Await.result(getApps, 60 seconds).get(appName).map(t => BinaryInfo(appName, t._1, t._2))
  }

  override def deleteBinary(appName: String): Unit = {
    binaries = binaries.filter { case ((name, _, _), _) => appName != name }
  }

  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
      Future[Seq[JobInfo]] = {
    throw new NotImplementedError()
  }
}
