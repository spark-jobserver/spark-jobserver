package spark.jobserver

import com.typesafe.config.Config
import java.io.{BufferedOutputStream, FileOutputStream}
import org.joda.time.DateTime
import scala.collection.mutable
import spark.jobserver.io.{JobDAO, JobInfo}
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
/**
 * In-memory DAO for easy unit testing
 */
class InMemoryDAO extends JobDAO {
  val jars = mutable.HashMap.empty[(String, DateTime), Array[Byte]]

  def saveJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]) {
    jars((appName, uploadTime)) = jarBytes
  }

  def getApps: Future[Map[String, DateTime]] = {
    Future {
      jars.keys
      .groupBy(_._1)
      .map { case (appName, appUploadTimeTuples) =>
        appName -> appUploadTimeTuples.map(_._2).toSeq.head
      }
    }
  }

  def retrieveJarFile(appName: String, uploadTime: DateTime): String = {
    // Write the jar bytes to a temporary file
    val outFile = java.io.File.createTempFile("InMemoryDAO", ".jar")
    outFile.deleteOnExit()
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      bos.write(jars((appName, uploadTime)))
    } finally {
      bos.close()
    }
    outFile.getAbsolutePath
  }

  val jobInfos = mutable.HashMap.empty[String, JobInfo]

  def saveJobInfo(jobInfo: JobInfo) { jobInfos(jobInfo.jobId) = jobInfo }

  def getJobInfos(limit: Int): Future[Seq[JobInfo]] = Future {
    jobInfos.values.toSeq.sortBy(_.startTime.toString()).take(limit)
  }

  def getJobInfo(jobId: String): Future[Option[JobInfo]] = Future {
    jobInfos.get(jobId)
  }

  val jobConfigs = mutable.HashMap.empty[String, Config]

  def saveJobConfig(jobId: String, jobConfig: Config) { jobConfigs(jobId) = jobConfig }

  def getJobConfigs: Future[Map[String, Config]] = Future {
    jobConfigs.toMap
  }
}
