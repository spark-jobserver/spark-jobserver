package spark.jobserver.io

import com.typesafe.config._
import org.joda.time.{DateTime, Duration}
import spray.http.{HttpHeaders, MediaType, MediaTypes}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

trait BinaryType {
  def extension: String
  def name: String
  def mediaType: MediaType
}
object BinaryType {

  case object Jar extends BinaryType {
    val extension = "jar"
    val name = "Jar"
    val mediaType: MediaType = MediaTypes.register(MediaType.custom("application/java-archive"))
    val contentType = HttpHeaders.`Content-Type`(mediaType)
  }

  case object Egg extends BinaryType {
    val extension = "egg"
    val name = "Egg"
    val mediaType: MediaType = MediaTypes.register(MediaType.custom("application/python-archive"))
    val contentType = HttpHeaders.`Content-Type`(mediaType)
  }

  def fromString(typeString: String): BinaryType = typeString match {
    case "Jar" => Jar
    case "Egg" => Egg
  }

  def fromMediaType(mediaType: MediaType): Option[BinaryType] = mediaType match {
    case m if m == Jar.mediaType => Some(Jar)
    case m if m == Egg.mediaType => Some(Egg)
    case _ => None
  }
}

// Uniquely identifies the binary used to run a job
case class BinaryInfo(appName: String, binaryType: BinaryType, uploadTime: DateTime)


// Both a response and used to track job progress
// NOTE: if endTime is not None, then the job has finished.
case class JobInfo(jobId: String, contextName: String,
                   binaryInfo: BinaryInfo, classPath: String,
                   startTime: DateTime, endTime: Option[DateTime],
                   error: Option[Throwable]) {
  def jobLengthMillis: Option[Long] = endTime.map { end => new Duration(startTime, end).getMillis }

  def isRunning: Boolean = endTime.isEmpty
  def isErroredOut: Boolean = endTime.isDefined && error.isDefined
}

object JobStatus {
  val Running = "RUNNING"
  val Error = "ERROR"
  val Finished = "FINISHED"
  val Started = "STARTED"
  val Killed = "KILLED"
}

/**
 * Core trait for data access objects for persisting data such as jars, applications, jobs, etc.
 */
trait JobDAO {
  /**
   * Persist a jar.
   *
   * @param appName
   * @param uploadTime
   * @param binaryBytes
   */
  def saveBinary(appName: String, binaryType: BinaryType, uploadTime: DateTime, binaryBytes: Array[Byte])

  /**
    * Delete a jar.
    * @param appName
    */
  def deleteBinary(appName: String)
  /**
   * Return all applications name and their last upload times.
   *
   * @return
   */
  def getApps: Future[Map[String, (BinaryType, DateTime)]]

  /**
   * TODO(kelvinchu): Remove this method later when JarManager doesn't use it anymore.
   *
   * @param appName
   * @param uploadTime
   * @return the local file path of the retrieved binary file.
   */
  def retrieveBinaryFile(appName: String, binaryType: BinaryType, uploadTime: DateTime): String

  /**
   * Persist a job info.
   *
   * @param jobInfo
   */
  def saveJobInfo(jobInfo: JobInfo)

  /**
   * Return job info for a specific job id.
   *
   * @return
   */
  def getJobInfo(jobId: String): Future[Option[JobInfo]]

  /**
   * Return all job ids to their job info.
   *
   * @return
   */
  def getJobInfos(limit: Int, status:Option[String] = None): Future[Seq[JobInfo]]

  /**
   * Persist a job configuration along with provided jobId.
   *
   * @param jobId
   * @param jobConfig
   */
  def saveJobConfig(jobId: String, jobConfig: Config)

  /**
   * Return all job ids to their job configuration.
   * @todo remove. used only in test
   * @return
   */
  def getJobConfigs: Future[Map[String, Config]]

  /**
   * Returns the last upload time for a given app name.
   * @return Some(lastUploadedTime) if the app exists and the list of times is nonempty, None otherwise
   */
  def getLastUploadTimeAndType(appName: String): Option[(DateTime, BinaryType)] =
    Await.result(getApps, 60 seconds).get(appName).map(t => (t._2, t._1))

  /**
    * Fetch submited jar or egg content for remote driver and JobManagerActor to cache in local
    * @param appName
    * @param uploadTime
    * @return
    */
  def getBinaryContent(appName: String,
                       binaryType: BinaryType,
                       uploadTime: DateTime): Array[Byte]
}
