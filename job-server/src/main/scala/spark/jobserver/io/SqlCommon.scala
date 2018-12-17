package spark.jobserver.io

import java.sql.Timestamp
import javax.sql.DataSource

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.reflect.runtime.universe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import org.apache.commons.dbcp.BasicDataSource
import org.flywaydb.core.Flyway
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import slick.driver.JdbcProfile

class SqlCommon(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  val slickDriverClass = config.getString("spark.jobserver.sqldao.slick-driver")
  val jdbcDriverClass = config.getString("spark.jobserver.sqldao.jdbc-driver")

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profileModule = runtimeMirror.staticModule(slickDriverClass)
  val profile = runtimeMirror.reflectModule(profileModule).instance.asInstanceOf[JdbcProfile]

  import profile.api._

  // Definition of the tables
  //scalastyle:off
  // Explicitly avoiding to label 'jarId' as a foreign key to avoid dealing with
  // referential integrity constraint violations.
  class Jobs(tag: Tag) extends Table[(String, String, String, Int, String, String, Timestamp,
    Option[Timestamp], Option[String], Option[String], Option[String])](tag, "JOBS") {
    def jobId = column[String]("JOB_ID", O.PrimaryKey)
    def contextId = column[String]("CONTEXT_ID")
    def contextName = column[String]("CONTEXT_NAME")
    def binId = column[Int]("BIN_ID")
    def classPath = column[String]("CLASSPATH")
    def state = column[String]("STATE")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Option[Timestamp]]("END_TIME")
    def error = column[Option[String]]("ERROR")
    def errorClass = column[Option[String]]("ERROR_CLASS")
    def errorStackTrace = column[Option[String]]("ERROR_STACK_TRACE")
    def * = (jobId, contextId, contextName, binId, classPath, state, startTime, endTime,
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

  // DB initialization
  val jdbcUrl = config.getString("spark.jobserver.sqldao.jdbc.url")
  val jdbcUser = config.getString("spark.jobserver.sqldao.jdbc.user")
  val jdbcPassword = config.getString("spark.jobserver.sqldao.jdbc.password")
  val enableDbcp = config.getBoolean("spark.jobserver.sqldao.dbcp.enabled")
  val db = if (enableDbcp) {
    logger.info("DBCP enabled")
    val dbcpMaxActive = config.getInt("spark.jobserver.sqldao.dbcp.maxactive")
    val dbcpMaxIdle = config.getInt("spark.jobserver.sqldao.dbcp.maxidle")
    val dbcpInitialSize = config.getInt("spark.jobserver.sqldao.dbcp.initialsize")
    val dataSource: DataSource = {
      val ds = new BasicDataSource
      ds.setDriverClassName(jdbcDriverClass)
      ds.setUsername(jdbcUser)
      ds.setPassword(jdbcPassword)
      ds.setMaxActive(dbcpMaxActive)
      ds.setMaxIdle(dbcpMaxIdle)
      ds.setInitialSize(dbcpInitialSize)
      ds.setUrl(jdbcUrl)
      ds
    }
    Database.forDataSource(dataSource)
  } else {
    logger.info("DBCP disabled")
    Database.forURL(jdbcUrl, driver = jdbcDriverClass, user = jdbcUser, password = jdbcPassword)
  }

  def initFlyway() {
    // TODO: migrateLocations should be removed when tests have a running configuration
    val migrateLocations = config.getString("flyway.locations")
    val initOnMigrate = config.getBoolean("flyway.initOnMigrate")

    // Flyway migration
    val flyway = new Flyway()
    flyway.setDataSource(jdbcUrl, jdbcUser, jdbcPassword)
    // TODO: flyway.setLocations(migrateLocations) should be removed when tests have a running configuration
    flyway.setLocations(migrateLocations)
    flyway.setBaselineOnMigrate(initOnMigrate)
    flyway.migrate()
  }

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

  def jobInfoFromRow(row: (String, String, String, String, String,
    Timestamp, String, String, Timestamp, Option[Timestamp],
    Option[String], Option[String], Option[String])): JobInfo = row match {
    case (id, contextId, contextName, app, binType, upload, classpath,
        state, start, end, err, errCls, errStTr) =>
      val errorInfo = err.map(ErrorData(_, errCls.getOrElse(""), errStTr.getOrElse("")))
      JobInfo(
        id,
        contextId,
        contextName,
        BinaryInfo(app, BinaryType.fromString(binType), convertDateSqlToJoda(upload)),
        classpath,
        state,
        convertDateSqlToJoda(start),
        end.map(convertDateSqlToJoda),
        errorInfo
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
    val joinQuery = for {
      bin <- binaries
      j <- jobs if (status match {
                          case Some(state) => j.binId === bin.binId && j.state === state
                          case None => j.binId === bin.binId
                })
    } yield {
      (j.jobId, j.contextId, j.contextName, bin.appName, bin.binaryType, bin.uploadTime, j.classPath,
          j.state, j.startTime, j.endTime, j.error, j.errorClass, j.errorStackTrace)
    }
    val sortQuery = joinQuery.sortBy(_._9.desc)
    val limitQuery = sortQuery.take(limit)
    // Transform the each row of the table into a map of JobInfo values
    for (r <- db.run(limitQuery.result)) yield {
      r.map(jobInfoFromRow)
    }
  }

  def getJobsByContextId(contextId: String, statuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = {
    val joinQuery = for {
      bin <- binaries
      j <- jobs if ((contextId, statuses) match {
                          case (contextId, Some(s)) => j.binId === bin.binId &&
                              j.contextId === contextId && j.state.inSet(s)
                          case _ => j.binId === bin.binId && j.contextId === contextId
                })
    } yield {
      (j.jobId, j.contextId, j.contextName, bin.appName, bin.binaryType, bin.uploadTime, j.classPath,
          j.state, j.startTime, j.endTime, j.error, j.errorClass, j.errorStackTrace)
    }
    db.run(joinQuery.result).map(_.map(jobInfoFromRow))
  }

  def getJob(id: String): Future[Option[JobInfo]] = {
    // Join the BINARIES and JOBS tables without unnecessary columns
    val joinQuery = for {
      bin <- binaries
      j <- jobs if j.binId === bin.binId && j.jobId === id
    } yield {
      (j.jobId, j.contextId, j.contextName, bin.appName, bin.binaryType, bin.uploadTime, j.classPath,
         j.state, j.startTime, j.endTime, j.error, j.errorClass, j.errorStackTrace)
    }
    for (r <- db.run(joinQuery.result)) yield {
      r.map(jobInfoFromRow).headOption
    }
  }
}