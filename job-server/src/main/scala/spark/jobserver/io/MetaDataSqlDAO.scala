package spark.jobserver.io

import java.sql.Timestamp

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.reflect.runtime.universe

import com.typesafe.config.Config
import org.flywaydb.core.Flyway
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import slick.driver.JdbcProfile

class MetaDataSqlDAO(config: Config) extends MetaDataDAO {
  private val logger = LoggerFactory.getLogger(getClass)
  val sqlCommon: SqlCommon = new SqlCommon(config)

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profile = runtimeMirror.reflectModule(sqlCommon.profileModule).instance.asInstanceOf[JdbcProfile]

  import profile.api._

  // TODO: migrateLocations should be removed when tests have a running configuration
  val migrateLocations = config.getString("flyway.locations")
  val initOnMigrate = config.getBoolean("flyway.initOnMigrate")

  // Server initialization
  init()

  /** Persist a context info.
    *
    * @param contextInfo
    */
  def saveContext(contextInfo: ContextInfo): Future[Boolean] = {
    sqlCommon.saveContext(contextInfo)
  }

  /**
    * Return context info for a specific context id.
    *
    * @return
    */
  def getContext(id: String): Future[Option[ContextInfo]] = {
    sqlCommon.getContext(id)
  }

  /**
    * Return context info for a specific context name.
    *
    * @return
    */
  def getContextByName(name: String): Future[Option[ContextInfo]] = {
    sqlCommon.getContextByName(name)
  }

  /**
    * Return context info for a "limit" number of contexts and specific statuses if given.
    * If limit and statuses are not provided, return context info of all active contexts.
    *
    * @return
    */
  def getContexts(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
  Future[Seq[ContextInfo]] = {
    sqlCommon.getContexts(limit, statuses)
  }

  /**
    * Persist a job info.
    *
    * @param jobInfo
    */
  def saveJob(jobInfo: JobInfo): Future[Boolean] = {
    val binId =
      Await.result(
        sqlCommon.queryBinaryId(
          jobInfo.binaryInfo.appName,
          jobInfo.binaryInfo.binaryType,
          jobInfo.binaryInfo.uploadTime),
        60 seconds)
    val startTime = sqlCommon.convertDateJodaToSql(jobInfo.startTime)
    val endTime = jobInfo.endTime.map(t => sqlCommon.convertDateJodaToSql(t))
    val error = jobInfo.error.map(e => e.message)
    val errorClass = jobInfo.error.map(e => e.errorClass)
    val errorStackTrace = jobInfo.error.map(e => e.stackTrace)
    val row = (jobInfo.jobId, jobInfo.contextId, jobInfo.contextName, binId, jobInfo.classPath,
      jobInfo.state, startTime, endTime, error, errorClass, errorStackTrace)
    val result = for {
      result <- sqlCommon.db.run(sqlCommon.jobs.insertOrUpdate(row))
    } yield {
      if (result == 0) {
        val e = new SlickException(s"Could not update ${jobInfo.jobId} in the database")
        logger.error(e.getMessage, e)
        false
      } else {
        true
      }
    }
    result
  }

  /**
    * Return job info for a specific job id.
    *
    * @return
    */
  def getJob(id: String): Future[Option[JobInfo]] = {
    sqlCommon.getJob(id)
  }

  /**
    * Return job info for a "limit" number of jobs and specific "status" if given.
    *
    * @return
    */
  def getJobs(limit: Int, status: Option[String] = None): Future[Seq[JobInfo]] = {
    sqlCommon.getJobs(limit, status)
  }

  /**
    * Return all job ids to their job info.
    */
  def getJobsByContextId(contextId: String, statuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = {
    sqlCommon.getJobsByContextId(contextId, statuses)
  }

  /**
    * Persist a job configuration along with provided job id.
    *
    * @param id
    * @param config
    */
  def saveJobConfig(id: String, config: Config): Future[Boolean] = {
    sqlCommon.saveJobConfig(id, config)
  }

  /**
    * Returns a config for a given job id
    * @return
    */
  def getJobConfig(id: String): Future[Option[Config]] = {
    sqlCommon.getJobConfig(id)
  }

  /**
    * Get meta information about the last uploaded binary with a given name.
    *
    * @param name binary name
    */
  def getBinary(name: String): Future[Option[BinaryInfo]] = {
    val query = sqlCommon.binaries.filter(_.appName === name).sortBy(_.uploadTime.desc).result
    sqlCommon.db.run(query.headOption).map(r => r.map(binaryInfoFromRow(_)))
  }

  /**
    * Return all binaries names and their last upload times.
    *
    * @return
    */
  def getBinaries: Future[Seq[BinaryInfo]] = {
    val query = sqlCommon.binaries.groupBy { r =>
      (r.appName, r.binaryType)
    }.map {
      case ((name, binaryType), bin) =>
        (name, binaryType, bin.map(_.uploadTime).max.get)
    }.result
    for (m <- sqlCommon.db.run(query)) yield {
      m.map {
        case (appName, binaryType, uploadTime) =>
          BinaryInfo(
            appName,
            BinaryType.fromString(binaryType),
            sqlCommon.convertDateSqlToJoda(uploadTime)
          )
      }
    }
  }

    /**
    * Return info for all binaries with the given storage id.
    *
    * @return
    */
  def getBinariesByStorageId(storageId: String): Future[Seq[BinaryInfo]] = {
    val query = sqlCommon.binaries.filter(
          _.binHash === BinaryDAO.hashStringToBytes(storageId)
        ).result
    for (m <- sqlCommon.db.run(query)) yield {
      m.map(binaryInfoFromRow)
    }
  }

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
                 binaryStorageId: String): Future[Boolean] = {
    val hash = BinaryDAO.hashStringToBytes(binaryStorageId)
    val dbAction = (sqlCommon.binaries +=
      (-1, name, binaryType.name, sqlCommon.convertDateJodaToSql(uploadTime), hash))
    sqlCommon.db.run(dbAction).map(_ == 1).recover(logDeleteErrors)
  }

  /**
    * Delete meta information about a jar.
    * @param name
    */
  def deleteBinary(name: String): Future[Boolean] = {
    val deleteBinary = sqlCommon.binaries.filter(_.appName === name).delete
    sqlCommon.db.run(deleteBinary).map(_ > 0).recover(logDeleteErrors)
  }

  private def init() {
    // Flyway migration
    val flyway = new Flyway()
    flyway.setDataSource(sqlCommon.jdbcUrl, sqlCommon.jdbcUser, sqlCommon.jdbcPassword)
    // TODO: flyway.setLocations(migrateLocations) should be removed when tests have a running configuration
    flyway.setLocations(migrateLocations)
    flyway.setBaselineOnMigrate(initOnMigrate)
    flyway.migrate()
  }

  private def binaryInfoFromRow(row: (Int, String, String, Timestamp, Array[Byte])): BinaryInfo = row match {
    case (binId, appName, binaryType, uploadTime, hash) =>
      BinaryInfo(
        appName,
        BinaryType.fromString(binaryType),
        sqlCommon.convertDateSqlToJoda(uploadTime),
        Some(BinaryDAO.hashBytesToString(hash))
      )
  }

  private def logDeleteErrors = PartialFunction[Any, Boolean] {
    case e: Throwable =>
      logger.error(e.getMessage, e)
      false
  }
}