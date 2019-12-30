package spark.jobserver.io

import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.util.SqlDBUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

/**
 * Common SQL methods for MetaDataSqlDAO and JobSqlDAO. Deprecated and planned
 * to be removed, shouldn't be used in any new scenarios.
 */
@Deprecated
class SqlCommon(config: Config) extends SqlDBUtils(config){
  private val logger = LoggerFactory.getLogger(getClass)
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
  //scalastyle:on

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
    db.run(query.head)
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

  def jobInfoFromRow(row: (String, String, String, String, String, String, String,
  Timestamp, Option[Timestamp], Option[String], Option[String], Option[String])): JobInfo = row match {
    case (id, contextId, contextName, bins, uris, classpath,
        state, start, end, err, errCls, errStTr) =>
      val errorInfo = err.map(ErrorData(_
        , errCls.getOrElse(""), errStTr.getOrElse("")))

      val binIds = bins.split(",").toSeq.map(_.toInt)
      val query = binaries.filter(_.binId inSet binIds).result
      val binInfos = Await.result(
        db.run(query).map(r => r.map(binaryInfoFromRow(_))), timeout)
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

  def saveContext(contextInfo: ContextInfo): Future[Boolean] = {
    val startTime = convertDateJodaToSql(contextInfo.startTime)
    val endTime = contextInfo.endTime.map(t => convertDateJodaToSql(t))
    val errors = contextInfo.error.map(e => e.getMessage)
    val row = (contextInfo.id, contextInfo.name, contextInfo.config,
                contextInfo.actorAddress, startTime, endTime, contextInfo.state, errors)
    for {
      result <- db.run(contexts.insertOrUpdate(row))
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

  def getContext(id: String): Future[Option[ContextInfo]] = {
    val query = contexts.filter(_.id === id).result
    db.run(query.headOption).map(r => r.map(contextInfoFromRow(_)))
  }

  def getContextByName(name: String): Future[Option[ContextInfo]] = {
    val query = contexts.filter(_.name === name).sortBy(_.startTime.desc).result
    db.run(query.headOption).map(r => r.map(contextInfoFromRow(_)))
  }

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
    for (r <- db.run(limitQuery.result)) yield {
      r.map(contextInfoFromRow)
    }
  }

  def saveJobConfig(id: String, config: Config): Future[Boolean] = {
    val configRender = config.root().render(ConfigRenderOptions.concise())
    for {
      result <- (db.run(configs.map(c => c.*) += (id, configRender)))
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

  def getJobConfig(id: String): Future[Option[Config]] = {
    val query = configs
      .filter(_.jobId === id).map(_.jobConfig).result

    db.run(query.headOption).map(c => c.map(ConfigFactory.parseString(_)))
  }

  def getJobs(limit: Int, status: Option[String] = None): Future[Seq[JobInfo]] = {
    val baseQuery = if (status.isDefined) {
      jobs.filter(_.state === status)
    } else {
      jobs
    }
    val limitQuery = baseQuery.sortBy(_.startTime).take(limit)
    // Transform the each row of the table into a map of JobInfo values
    for (r <- db.run(limitQuery.result)) yield {
      r.map(jobInfoFromRow)
    }
  }

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
    db.run(joinQuery.result).map(_.map(jobInfoFromRow))
  }

  def getJob(id: String): Future[Option[JobInfo]] = {
    for (r <- db.run(jobs.filter(j => j.jobId === id).result)) yield {
      r.map(jobInfoFromRow).headOption
    }
  }

  /**
    * @param binName The name of the binary to be looked up
    * @param statuses List of job statuses to look up
    * @return List of found jobs
    */
  def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = {
    val binQuery = binaries.filter(_.appName === binName).map(_.binId).result.headOption
    val binId = Await.result(db.run(binQuery), 10.seconds).getOrElse(return Future.successful(Seq.empty))

    // .result should be called on jobs before uris/binaries string can be parsed
    // before it's only Rep[String], not real String
    val jobsQuery = jobs.result.map(_.filter {
      case (_, _, _, cp, _, _, status, _, _, _, _, _) =>
        cp.split(",").map(_.toInt).contains(binId) &&
          statuses.getOrElse(Seq(status)).contains(status)
    })

    db.run(jobsQuery).map(_.map(jobInfoFromRow))
  }
}