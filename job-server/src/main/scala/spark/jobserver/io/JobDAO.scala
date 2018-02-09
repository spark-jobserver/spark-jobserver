package spark.jobserver.io

import java.io.{PrintWriter, StringWriter}

import com.typesafe.config._
import org.joda.time.{DateTime, Duration}
import org.slf4j.LoggerFactory
import spark.jobserver.JobManagerActor.JobKilledException
import spray.http.{HttpHeaders, MediaType, MediaTypes}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

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

case class ErrorData(message: String, errorClass: String, stackTrace: String)

object ErrorData {
  def apply(ex: Throwable): ErrorData = {
    ErrorData(ex.getMessage, ex.getClass.getName, getStackTrace(ex))
  }

  def getStackTrace(ex: Throwable): String = {
    val stackTrace = new StringWriter()
    ex.printStackTrace(new PrintWriter(stackTrace))
    stackTrace.toString
  }
}

// Both a response and used to track job progress
// NOTE: if endTime is not None, then the job has finished.
case class JobInfo(jobId: String, contextName: String,
                   binaryInfo: BinaryInfo, classPath: String,
                   startTime: DateTime, endTime: Option[DateTime],
                   error: Option[ErrorData]) {
  def jobLengthMillis: Option[Long] = endTime.map { end => new Duration(startTime, end).getMillis }

  def isRunning: Boolean = endTime.isEmpty
  def isErroredOut: Boolean = endTime.isDefined && error.isDefined
}

case class ContextInfo(id: String, name: String,
                   config: String, actorAddress: Option[String],
                   startTime: DateTime, endTime: Option[DateTime],
                   state: String, error: Option[Throwable])

object JobStatus {
  val Running = "RUNNING"
  val Error = "ERROR"
  val Finished = "FINISHED"
  val Started = "STARTED"
  val Killed = "KILLED"
}

object ContextStatus {
  val Running = "RUNNING"
  val Error = "ERROR"
  val Stopping = "STOPPING"
  val Finished = "FINISHED"
  val Started = "STARTED"
  val Killed = "KILLED"
}

object JobDAO {
  private val logger = LoggerFactory.getLogger(classOf[JobDAO])
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
   * Persist a context info.
   *
   * @param contextInfo
   */
  def saveContextInfo(contextInfo: ContextInfo)

  /**
   * Return context info for a specific context id.
   *
   * @return
   */
  def getContextInfo(id: String): Future[Option[ContextInfo]]

   /**
   * Return context info for a specific context name.
   *
   * @return
   */
  def getContextInfoByName(name: String): Future[Option[ContextInfo]]

  /**
   * Return context info for a "limit" number of contexts.
   *
   * @return
   */
  def getContextInfos(limit: Option[Int] = None, statusOpt: Option[String] = None):
    Future[Seq[ContextInfo]]

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
  def getJobInfos(limit: Int, status: Option[String] = None): Future[Seq[JobInfo]]

  /**
    * Return all job ids to their job info.
    */
  def getRunningJobInfosForContextName(contextName: String): Future[Seq[JobInfo]]

  /**
    * Move all jobs running on context with given name to error state
    *
    * @param contextName name of the context
    * @param endTime time to put into job infos end time column
    */
  def cleanRunningJobInfosForContext(contextName: String, endTime: DateTime): Future[Unit] = {
    getRunningJobInfosForContextName(contextName).map { infos =>
      JobDAO.logger.info("cleaning {} running jobs for {}", infos.size, contextName)
      for (info <- infos) {
        val updatedInfo = info.copy(
          endTime = Some(endTime),
          error = Some(ErrorData(JobKilledException(info.jobId))))
        saveJobInfo(jobInfo = updatedInfo)
      }
    }
  }

  /**
   * Persist a job configuration along with provided jobId.
   *
   * @param jobId
   * @param jobConfig
   */
  def saveJobConfig(jobId: String, jobConfig: Config)

  /**
    * Returns a config for a given jobId
    * @return
    */
  def getJobConfig(jobId: String): Future[Option[Config]]

  /**
   * Returns the last upload time for a given app name.
   * @return Some(lastUploadedTime) if the app exists and the list of times is nonempty, None otherwise
   */
  def getLastUploadTimeAndType(appName: String): Option[(DateTime, BinaryType)]
}
