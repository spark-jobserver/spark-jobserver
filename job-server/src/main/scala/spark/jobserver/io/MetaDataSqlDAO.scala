package spark.jobserver.io

import java.sql.Timestamp

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.reflect.runtime.universe
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import slick.driver.JdbcProfile
import spark.jobserver.util.{ErrorData, SqlDBUtils}

class MetaDataSqlDAO(config: Config) extends MetaDataDAO {
  private val logger = LoggerFactory.getLogger(getClass)

  val dbUtils = new SqlDBUtils(config)
  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profile = runtimeMirror.reflectModule(dbUtils.profileModule).instance.asInstanceOf[JdbcProfile]

  private val timeout = 10.seconds
  import profile.api._
  // Definition of the tables
  //scalastyle:off
  // Explicitly avoiding to label 'jarId' as a foreign key to avoid dealing with
  // referential integrity constraint violations.
  class Jobs(tag: Tag) extends Table[(String, String, String, String, String, String, String, Timestamp,
    Option[Timestamp], Option[String], Option[String], Option[String])](tag, "JOBS") {
    def jobId = column[String]("JOB_ID", O.PrimaryKey)
    def contextId = column[String]("CONTEXT_ID")
    def contextName = column[String]("CONTEXT_NAME")
    def binIds = column[String]("BIN_IDS")
    def URIs = column[String]("URIS")
    def classPath = column[String]("CLASSPATH")
    def state = column[String]("STATE")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Option[Timestamp]]("END_TIME")
    def error = column[Option[String]]("ERROR")
    def errorClass = column[Option[String]]("ERROR_CLASS")
    def errorStackTrace = column[Option[String]]("ERROR_STACK_TRACE")
    def * = (jobId, contextId, contextName, binIds, URIs, classPath, state, startTime, endTime,
      error, errorClass, errorStackTrace)
  }

  val jobs = TableQuery[Jobs]

  class Contexts(tag: Tag) extends Table[(String, String, String, Option[String], Timestamp,
    Option[Timestamp], String, Option[String])](tag, "CONTEXTS") {
    def id = column[String]("ID", O.PrimaryKey)
    def name = column[String]("NAME")
    def config = column[String]("CONFIG")
    def actorAddress = column[Option[String]]("ACTOR_ADDRESS")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Option[Timestamp]]("END_TIME")
    def state = column[String]("STATE")
    def error = column[Option[String]]("ERROR")
    def * = (id, name, config, actorAddress, startTime, endTime, state, error)
  }

  val contexts = TableQuery[Contexts]

  class Configs(tag: Tag) extends Table[(String, String)](tag, "CONFIGS") {
    def jobId = column[String]("JOB_ID", O.PrimaryKey)
    def jobConfig = column[String]("JOB_CONFIG")
    def * = (jobId, jobConfig)
  }

  val configs = TableQuery[Configs]

  class Binaries(tag: Tag) extends Table[(Int, String, String, Timestamp, Array[Byte])](tag, "BINARIES") {
    def binId = column[Int]("BIN_ID", O.PrimaryKey, O.AutoInc)
    def appName = column[String]("APP_NAME")
    def binaryType = column[String]("BINARY_TYPE")
    def uploadTime = column[Timestamp]("UPLOAD_TIME")
    def binHash = column[Array[Byte]]("BIN_HASH")
    def * = (binId, appName, binaryType, uploadTime, binHash)
  }

  val binaries = TableQuery[Binaries]

  // Convert from joda DateTime to java.sql.Timestamp
  def convertDateJodaToSql(dateTime: DateTime): Timestamp = new Timestamp(dateTime.getMillis)

  // Convert from java.sql.Timestamp to joda DateTime
  def convertDateSqlToJoda(timestamp: Timestamp): DateTime = new DateTime(timestamp.getTime)

  private def contextInfoFromRow(row: (String, String, String, Option[String],
    Timestamp, Option[Timestamp], String, Option[String])): ContextInfo = row match {
    case (id, name, config, actorAddress, start, end, state, error) =>
      ContextInfo(
        id,
        name,
        config,
        actorAddress,
        convertDateSqlToJoda(start),
        end.map(convertDateSqlToJoda),
        state,
        error.map(new Throwable(_))
      )
  }

  def queryBinaryId(name: String, binaryType: BinaryType, uploadTime: DateTime): Future[Int] = {
    val dateTime = convertDateJodaToSql(uploadTime)
    val query = binaries.filter { bin =>
      bin.appName === name && bin.uploadTime === dateTime && bin.binaryType === binaryType.name
    }.map(_.binId).result
    dbUtils.db.run(query.head)
  }

  def jobInfoFromRow(row: (String, String, String, String, String, String, String,
    Timestamp, Option[Timestamp], Option[String], Option[String], Option[String])): JobInfo = row match {
    case (id, contextId, contextName, bins, uris, classpath,
    state, start, end, err, errCls, errStTr) =>
      val errorInfo = err.map(ErrorData(_
        , errCls.getOrElse(""), errStTr.getOrElse("")))

      val binIds = bins.split(",").toSeq.map(_.toInt)
      val query = binaries.filter(_.binId inSet binIds).result
      val binInfos = Await.result(
        dbUtils.db.run(query).map(r => r.map(binaryInfoFromRow(_))), timeout)
      val uriInfos = if (uris.nonEmpty) {
        uris.split(",").toSeq.map(u => BinaryInfo(u, BinaryType.URI, DateTime.now()))
      } else {
        Seq.empty
      }
      JobInfo(
        id,
        contextId,
        contextName,
        classpath,
        state,
        convertDateSqlToJoda(start),
        end.map(convertDateSqlToJoda),
        errorInfo,
        binInfos ++ uriInfos
      )
  }

  dbUtils.initFlyway()

  /** Persist a context info.
    *
    * @param contextInfo
    */
  def saveContext(contextInfo: ContextInfo): Future[Boolean] = {
    val startTime = convertDateJodaToSql(contextInfo.startTime)
    val endTime = contextInfo.endTime.map(t => convertDateJodaToSql(t))
    val errors = contextInfo.error.map(e => e.getMessage)
    val row = (contextInfo.id, contextInfo.name, contextInfo.config,
      contextInfo.actorAddress, startTime, endTime, contextInfo.state, errors)
    for {
      result <- dbUtils.db.run(contexts.insertOrUpdate(row))
    } yield {
      if (result == 0) {
        val e = new SlickException(s"Could not update ${contextInfo.id} in the database")
        logger.error(e.getMessage, e)
        false
      } else {
        true
      }
    }
  }

  /**
    * Return context info for a specific context id.
    *
    * @return
    */
  def getContext(id: String): Future[Option[ContextInfo]] = {
    val query = contexts.filter(_.id === id).result
    dbUtils.db.run(query.headOption).map(r => r.map(contextInfoFromRow(_)))
  }

  /**
    * Return context info for a specific context name.
    *
    * @return
    */
  def getContextByName(name: String): Future[Option[ContextInfo]] = {
    val query = contexts.filter(_.name === name).sortBy(_.startTime.desc).result
    dbUtils.db.run(query.headOption).map(r => r.map(contextInfoFromRow(_)))
  }

  /**
    * Return context info for a "limit" number of contexts and specific statuses if given.
    * If limit and statuses are not provided, return context info of all active contexts.
    *
    * @return
    */
  def getContexts(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
  Future[Seq[ContextInfo]] = {
    val query = statuses match {
      case Some(statuses) => contexts.filter(_.state.inSet(statuses))
      case None => contexts
    }
    val sortQuery = query.sortBy(_.startTime.desc)
    val limitQuery = limit match {
      case Some(i) => sortQuery.take(i)
      case None => sortQuery
    }
    for (r <- dbUtils.db.run(limitQuery.result)) yield {
      r.map(contextInfoFromRow)
    }
  }

  /**
    * Persist a job info.
    *
    * @param jobInfo
    */
  def saveJob(jobInfo: JobInfo): Future[Boolean] = {
    val jarIds = jobInfo.cp.filter(_.binaryType != BinaryType.URI).map(b => {
      Await.result(
        queryBinaryId(
          b.appName,
          b.binaryType,
          b.uploadTime),
        60.seconds)
    }).mkString(",")
    val uris = jobInfo.cp.filter(_.binaryType == BinaryType.URI).map(_.appName).mkString(",")
    val startTime = convertDateJodaToSql(jobInfo.startTime)
    val endTime = jobInfo.endTime.map(t => convertDateJodaToSql(t))
    val error = jobInfo.error.map(e => e.message)
    val errorClass = jobInfo.error.map(e => e.errorClass)
    val errorStackTrace = jobInfo.error.map(e => e.stackTrace)
    val row = (jobInfo.jobId, jobInfo.contextId, jobInfo.contextName, jarIds, uris, jobInfo.mainClass,
      jobInfo.state, startTime, endTime, error, errorClass, errorStackTrace)
    val result = for {
      result <- dbUtils.db.run(jobs.insertOrUpdate(row))
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
    for (r <- dbUtils.db.run(jobs.filter(j => j.jobId === id).result)) yield {
      r.map(jobInfoFromRow).headOption
    }
  }

  /**
    * Return job info for a "limit" number of jobs and specific "status" if given.
    *
    * @return
    */
  def getJobs(limit: Int, status: Option[String] = None): Future[Seq[JobInfo]] = {
    val baseQuery = if (status.isDefined) {
      jobs.filter(_.state === status)
    } else {
      jobs
    }
    val limitQuery = baseQuery.sortBy(_.startTime).take(limit)
    // Transform the each row of the table into a map of JobInfo values
    for (r <- dbUtils.db.run(limitQuery.result)) yield {
      r.map(jobInfoFromRow)
    }
  }

  /**
    * Return all job ids to their job info.
    */
  def getJobsByContextId(contextId: String, statuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = {
    val joinQuery = for {
      j <- jobs if ((contextId, statuses) match {
        case (contextId, Some(s)) =>
          j.contextId === contextId && j.state.inSet(s)
        case _ => j.contextId === contextId
      })
    } yield {
      j
    }
    dbUtils.db.run(joinQuery.result).map(_.map(jobInfoFromRow))
  }

  /**
    * Persist a job configuration along with provided job id.
    *
    * @param id
    * @param config
    */
  def saveJobConfig(id: String, config: Config): Future[Boolean] = {
    val configRender = config.root().render(ConfigRenderOptions.concise())
    for {
      result <- (dbUtils.db.run(configs.map(c => c.*) += (id, configRender)))
    } yield {
      if (result == 0) {
        val e = new SlickException(s"Could not insert $id into database")
        logger.error(e.getMessage, e)
        false
      } else {
        true
      }
    }
  }

  /**
    * Returns a config for a given job id
    * @return
    */
  def getJobConfig(id: String): Future[Option[Config]] = {
    val query = configs
      .filter(_.jobId === id).map(_.jobConfig).result

    dbUtils.db.run(query.headOption).map(c => c.map(ConfigFactory.parseString(_)))
  }

  /**
    * Get meta information about the last uploaded binary with a given name.
    *
    * @param name binary name
    */
  def getBinary(name: String): Future[Option[BinaryInfo]] = {
    val query = binaries.filter(_.appName === name).sortBy(_.uploadTime.desc).result
    dbUtils.db.run(query.headOption).map(r => r.map(binaryInfoFromRow(_)))
  }

  /**
    * Return all binaries names and their last upload times.
    *
    * @return
    */
  def getBinaries: Future[Seq[BinaryInfo]] = {
    val query = binaries.groupBy { r =>
      (r.appName, r.binaryType)
    }.map {
      case ((name, binaryType), bin) =>
        (name, binaryType, bin.map(_.uploadTime).max.get)
    }.result
    for (m <- dbUtils.db.run(query)) yield {
      m.map {
        case (appName, binaryType, uploadTime) =>
          BinaryInfo(
            appName,
            BinaryType.fromString(binaryType),
            convertDateSqlToJoda(uploadTime)
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
    val query = binaries.filter(
          _.binHash === BinaryDAO.hashStringToBytes(storageId)
        ).result
    for (m <- dbUtils.db.run(query)) yield {
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
    val dbAction = (binaries +=
      (-1, name, binaryType.name, convertDateJodaToSql(uploadTime), hash))
    dbUtils.db.run(dbAction).map(_ == 1).recover(logDeleteErrors)
  }

  /**
    * Delete meta information about a jar.
    * @param name
    */
  def deleteBinary(name: String): Future[Boolean] = {
    val deleteBinary = binaries.filter(_.appName === name).delete
    dbUtils.db.run(deleteBinary).map(_ > 0).recover(logDeleteErrors)
  }

  def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = {
    val binQuery = binaries.filter(_.appName === binName).map(_.binId).result.headOption
    val binId = Await.result(dbUtils.db.run(binQuery), 10.seconds).getOrElse(return Future.successful(Seq.empty))

    // .result should be called on jobs before uris/binaries string can be parsed
    // before it's only Rep[String], not real String
    val jobsQuery = jobs.result.map(_.filter {
      case (_, _, _, cp, _, _, status, _, _, _, _, _) =>
        cp.split(",").map(_.toInt).contains(binId) &&
          statuses.getOrElse(Seq(status)).contains(status)
    })

    dbUtils.db.run(jobsQuery).map(_.map(jobInfoFromRow))
  }

  private def binaryInfoFromRow(row: (Int, String, String, Timestamp, Array[Byte])): BinaryInfo = row match {
    case (binId, appName, binaryType, uploadTime, hash) =>
      BinaryInfo(
        appName,
        BinaryType.fromString(binaryType),
        convertDateSqlToJoda(uploadTime),
        Some(BinaryDAO.hashBytesToString(hash))
      )
  }

  private def logDeleteErrors = PartialFunction[Any, Boolean] {
    case e: Throwable =>
      logger.error(e.getMessage, e)
      false
  }
}