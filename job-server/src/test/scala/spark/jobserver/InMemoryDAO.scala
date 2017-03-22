package spark.jobserver

import com.typesafe.config.Config
import java.io.{BufferedOutputStream, FileOutputStream}

import org.joda.time.DateTime

import scala.collection.mutable
import spark.jobserver.io.{JobStatus, BinaryType, JobDAO, JobInfo}

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
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

  override def retrieveBinaryFile(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    // Write the jar bytes to a temporary file
    val outFile = java.io.File.createTempFile("InMemoryDAO", s".${binaryType.extension}")
    outFile.deleteOnExit()
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      bos.write(binaries((appName, binaryType, uploadTime)))
    } finally {
      bos.close()
    }
    outFile.getAbsolutePath
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

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = Future {
    jobInfos.get(jobId)
  }

  val jobConfigs = mutable.HashMap.empty[String, Config]

  override def saveJobConfig(jobId: String, jobConfig: Config) { jobConfigs(jobId) = jobConfig }

  override def getJobConfigs: Future[Map[String, Config]] = Future {
    jobConfigs.toMap
  }

  override def getBinaryContent(appName: String, binaryType: BinaryType,
                                uploadTime: DateTime): Array[Byte] = {
    binaries((appName, binaryType, uploadTime))
  }

  override def deleteBinary(appName: String): Unit = {
    binaries = binaries.filter { case ((name, _, _), _) => appName != name }
  }
}
