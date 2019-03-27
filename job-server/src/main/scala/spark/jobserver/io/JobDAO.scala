package spark.jobserver.io

import java.io.{PrintWriter, StringWriter}

import com.typesafe.config._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

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

object JobDAO {
  private val logger = LoggerFactory.getLogger(classOf[JobDAO])
}

/**
 * Core trait for data access objects for persisting data such as jars, applications, jobs, etc.
 */
trait JobDAO {
  /**
   * Persist a binary data.
   *
   * @param appName
   * @param uploadTime
   * @param binaryBytes
   */
  def saveBinary(appName: String, binaryType: BinaryType, uploadTime: DateTime, binaryBytes: Array[Byte])

  /**
    * Delete a binary data.
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
  def getBinaryFilePath(appName: String, binaryType: BinaryType, uploadTime: DateTime): String

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
  def getContextInfos(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
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
  def getJobInfosByContextId(contextId: String, jobStatuses: Option[Seq[String]] = None): Future[Seq[JobInfo]]

  /**
    * Move all jobs running on context with given name to error state
    *
    * @param contextName name of the context
    * @param endTime time to put into job infos end time column
    */
  def cleanRunningJobInfosForContext(contextId: String, endTime: DateTime): Future[Unit] = {
    import spark.jobserver.JobManagerActor.ContextTerminatedException
    getJobInfosByContextId(contextId, Some(Seq(JobStatus.Running))).map { infos =>
      JobDAO.logger.info("cleaning {} running jobs for {}", infos.size, contextId)
      for (info <- infos) {
        val updatedInfo = info.copy(
          endTime = Some(endTime),
          error = Some(ErrorData(ContextTerminatedException(contextId))))
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
  def getBinaryInfo(appName: String): Option[BinaryInfo]
}
