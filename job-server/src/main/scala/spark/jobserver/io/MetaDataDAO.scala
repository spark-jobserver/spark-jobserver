package spark.jobserver.io

import com.typesafe.config._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object MetaDataDAO {
  private val logger = LoggerFactory.getLogger(getClass)
}

/**
  * Core trait for data access objects for persisting binaries/contexts/jobs meta data.
  */
trait MetaDataDAO {
  /**
    * Persist a context info.
    *
    * @param contextInfo
    */
  def saveContext(contextInfo: ContextInfo): Future[Boolean]

  /**
    * Return context info for a specific context id.
    *
    * @return
    */
  def getContext(id: String): Future[Option[ContextInfo]]

  /**
    * Return context info for a specific context name.
    *
    * @return
    */
  def getContextByName(name: String): Future[Option[ContextInfo]]

  /**
    * Return context info for a "limit" number of contexts and specific statuses if given.
    * If limit and statuses are not provided, return context info of all active contexts.
    *
    * @return
    */
  def getContexts(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
  Future[Seq[ContextInfo]]

  /**
    * Persist a job info.
    *
    * @param jobInfo
    */
  def saveJob(jobInfo: JobInfo): Future[Boolean]

  /**
    * Return job info for a specific job id.
    *
    * @return
    */
  def getJob(id: String): Future[Option[JobInfo]]

  /**
    * Return job info for a "limit" number of jobs and specific "status" if given.
    *
    * @return
    */
  def getJobs(limit: Int, status: Option[String] = None): Future[Seq[JobInfo]]

  /**
    * Return all job ids to their job info.
    */
  def getJobsByContextId(contextId: String, statuses: Option[Seq[String]] = None): Future[Seq[JobInfo]]

  /**
    * Persist a job configuration along with provided job id.
    *
    * @param id
    * @param config
    */
  def saveJobConfig(id: String, config: Config): Future[Boolean]

  /**
    * Returns a config for a given job id
    * @return
    */
  def getJobConfig(id: String): Future[Option[Config]]

  /**
    * Get meta information about the last uploaded binary with a given name.
    *
    * @param name binary name
    */
  def getBinary(name: String): Future[Option[BinaryInfo]]

  /**
    * Return info for all binaries.
    *
    * @return
    */
  def getBinaries: Future[Seq[BinaryInfo]]

  /**
    * Persist meta information about binary.
    *
    * @param name
    * @param uploadTime
    * @param binaryStorageId unique binary identifier used to save the binary
    */
  def saveBinary(name: String,
                 binaryType: BinaryType,
                 uploadTime: DateTime,
                 binaryStorageId: String): Future[Boolean]

  /**
    * Delete meta information about a jar.
    * @param name
    */
  def deleteBinary(name: String): Future[Boolean]
}
