package spark.jobserver.io

import java.io.File
import java.sql.{Blob, Timestamp}
import javax.sql.DataSource
import javax.sql.rowset.serial.SerialBlob

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
import slick.lifted.ProvenShape.proveShapeOf
import spark.jobserver.JobManagerActor.ContextTerminatedException
import spray.http.ErrorInfo
import spark.jobserver.util.NoSuchBinaryException

class JobSqlDAO(config: Config) extends JobDAO with FileCacher {
  val slickDriverClass = config.getString("spark.jobserver.sqldao.slick-driver")
  val jdbcDriverClass = config.getString("spark.jobserver.sqldao.jdbc-driver")

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profileModule = runtimeMirror.staticModule(slickDriverClass)
  val profile = runtimeMirror.reflectModule(profileModule).instance.asInstanceOf[JdbcProfile]

  import profile.api._

  private val logger = LoggerFactory.getLogger(getClass)

  // NOTE: below is only needed for H2 drivers
  val rootDir = config.getString("spark.jobserver.sqldao.rootdir")
  val rootDirFile = new File(rootDir)
  logger.info("rootDir is " + rootDirFile.getAbsolutePath)

  // Definition of the tables
  //scalastyle:off
  class Binaries(tag: Tag) extends Table[(Int, String, String, Timestamp, Array[Byte])](tag, "BINARIES") {
    def binId = column[Int]("BIN_ID", O.PrimaryKey, O.AutoInc)
    def appName = column[String]("APP_NAME")
    def binaryType = column[String]("BINARY_TYPE")
    def uploadTime = column[Timestamp]("UPLOAD_TIME")
    def binHash = column[Array[Byte]]("BIN_HASH")
    def * = (binId, appName, binaryType, uploadTime, binHash)
  }

  class BinariesContents(tag: Tag) extends Table[(Array[Byte], Blob)](tag, "BINARIES_CONTENTS") {
    def binHash = column[Array[Byte]]("BIN_HASH", O.PrimaryKey)
    def binary = column[Blob]("BINARY")
    def * = (binHash, binary)
  }

  val binaries = TableQuery[Binaries]
  val binariesContents = TableQuery[BinariesContents]

  // Explicitly avoiding to label 'jarId' as a foreign key to avoid dealing with
  // referential integrity constraint violations.
  class Jobs(tag: Tag) extends Table[(String, String, String, Int, String, String, Timestamp,
    Option[Timestamp], Option[String], Option[String], Option[String])](tag, "JOBS") {
    def jobId = column[String]("JOB_ID", O.PrimaryKey)
    def contextId = column[String]("CONTEXT_ID")
    def contextName = column[String]("CONTEXT_NAME")
    def binId = column[Int]("BIN_ID")
    // FK to JARS table
    def classPath = column[String]("CLASSPATH")
    def state = column[String]("STATE")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Option[Timestamp]]("END_TIME")
    def error = column[Option[String]]("ERROR")
    def errorClass = column[Option[String]]("ERROR_CLASS")
    def errorStackTrace = column[Option[String]]("ERROR_STACK_TRACE")
    def * = (jobId, contextId, contextName, binId, classPath, state, startTime, endTime, error, errorClass, errorStackTrace)
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

  //scalastyle:on
  val configs = TableQuery[Configs]

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
  // TODO: migrateLocations should be removed when tests have a running configuration
  val migrateLocations = config.getString("flyway.locations")
  val initOnMigrate = config.getBoolean("flyway.initOnMigrate")

  // Server initialization
  init()

  private def init() {
    // Create the data directory if it doesn't exist
    initFileDirectory()

    // Flyway migration
    val flyway = new Flyway()
    flyway.setDataSource(jdbcUrl, jdbcUser, jdbcPassword)
    // TODO: flyway.setLocations(migrateLocations) should be removed when tests have a running configuration
    flyway.setLocations(migrateLocations)
    flyway.setBaselineOnMigrate(initOnMigrate)
    flyway.migrate()
  }

  override def saveBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          binBytes: Array[Byte]) {
    val cacheOnUploadEnabled = config.getBoolean("spark.jobserver.cache-on-upload")
    if (cacheOnUploadEnabled) {
      // The order is important. Save the jar file first and then log it into database.
      cacheBinary(appName, binaryType, uploadTime, binBytes)
    }

    // log it into database
    if (Await.result(insertBinaryInfo(
      BinaryInfo(appName, binaryType, uploadTime),
      binBytes), 60 seconds) == 0) {
      throw new SlickException(s"Failed to insert binary: $appName " +
        s"of type ${binaryType.name} into database at $uploadTime")
    }
  }

  /**
    * Delete a jar.
    *
    * @param appName
    */
  override def deleteBinary(appName: String): Unit = {
    if (Await.result(deleteBinaryInfo(appName), 60 seconds) == 0) {
      throw new NoSuchBinaryException(appName)
    }
    cleanCacheBinaries(appName)
  }

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = {
    val query = binaries.groupBy { r =>
      (r.appName, r.binaryType)
    }.map {
      case ((appName, binaryType), bin) =>
        (appName, binaryType, bin.map(_.uploadTime).max.get)
    }.result
    for (m <- db.run(query)) yield {
      m.map {
        case (appName, binaryType, uploadTime) =>
          (appName, (BinaryType.fromString(binaryType), convertDateSqlToJoda(uploadTime)))
      }.toMap
    }
  }

  override def getLastUploadTimeAndType(appName: String): Option[(DateTime, BinaryType)] = {
    val query = binaries.filter(_.appName === appName)
      .sortBy(_.uploadTime.desc)
      .map(b => (b.uploadTime, b.binaryType)).result
      .map{_.headOption.map(b => (convertDateSqlToJoda(b._1), BinaryType.fromString(b._2)))}
    Await.result(db.run(query), 60 seconds)
  }

  private def calculateBinaryHash(binBytes: Array[Byte]): Array[Byte] = {
    import java.security.MessageDigest
    val md = MessageDigest.getInstance("SHA-256");
    md.digest(binBytes)
  }

  // Insert JarInfo and its jar into db and return the primary key associated with that row
  private def insertBinaryInfo(binInfo: BinaryInfo, binBytes: Array[Byte]): Future[Int] = {
    val hash = calculateBinaryHash(binBytes);
    val dbAction = (binaries +=
        (-1, binInfo.appName, binInfo.binaryType.name, convertDateJodaToSql(binInfo.uploadTime), hash))
                     .andThen(binariesContents.filter(_.binHash === hash).map(_.binHash)
                         .result.headOption.flatMap {
                       case Some(bc) => DBIO.successful(1)
                       case None => binariesContents += (hash, new SerialBlob(binBytes))
                     }).transactionally
    db.run(dbAction)
  }

  private def logDeleteErrors = PartialFunction[Any, Int] {
    case e: Throwable => logger.error(e.getMessage, e); 0
    case c: Int => c
  }

  private def deleteBinaryInfo(appName: String): Future[Int] = {
    val deleteBinary = binaries.filter(_.appName === appName)
    val hashUsed = binaries.filter(_.binHash in deleteBinary.map(_.binHash)).filter(_.appName =!= appName)
    val deleteBinariesContents = binariesContents.filter(_.binHash in deleteBinary.map(_.binHash))
    val dbAction = (for {
      _ <- hashUsed.result.headOption.flatMap{
        case None =>
          deleteBinariesContents.delete
        case Some(bc) =>
          DBIO.successful(None) // no-op
      }
      b <- deleteBinary.delete
    } yield b).transactionally
    db.run(dbAction).recover(logDeleteErrors)
  }

  override def retrieveBinaryFile(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    val binFile = new File(rootDir, createBinaryName(appName, binaryType, uploadTime))
    if (!binFile.exists()) {
      fetchAndCacheBinFile(appName, binaryType, uploadTime)
    }
    binFile.getAbsolutePath
  }

  // Fetch the jar file from database and cache it into local file system.
  private def fetchAndCacheBinFile(appName: String, binaryType: BinaryType, uploadTime: DateTime) {
    val jarBytes = Await.result(fetchBinary(appName, binaryType, uploadTime), 60 seconds)
    cacheBinary(appName, binaryType, uploadTime, jarBytes)
  }

  // Fetch the jar from the database
  private def fetchBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime): Future[Array[Byte]] = {
    val dateTime = convertDateJodaToSql(uploadTime)
    val query = for {
      b <- binaries.filter { bin =>
        bin.appName === appName && bin.uploadTime === dateTime && bin.binaryType === binaryType.name
      }
      bc <- binariesContents if b.binHash === bc.binHash
    } yield bc.binary
    val dbAction = query.result
    db.run(dbAction.head.map { b => b.getBytes(1, b.length.toInt) }.transactionally)
  }

  private def queryBinaryId(appName: String, binaryType: BinaryType, uploadTime: DateTime): Future[Int] = {
    val dateTime = convertDateJodaToSql(uploadTime)
    val query = binaries.filter { bin =>
      bin.appName === appName && bin.uploadTime === dateTime && bin.binaryType === binaryType.name
    }.map(_.binId).result
    db.run(query.head)
  }

  override def saveContextInfo(contextInfo: ContextInfo): Unit = {
    val startTime = convertDateJodaToSql(contextInfo.startTime)
    val endTime = contextInfo.endTime.map(t => convertDateJodaToSql(t))
    val errors = contextInfo.error.map(e => e.getMessage)
    val row = (contextInfo.id, contextInfo.name, contextInfo.config,
                contextInfo.actorAddress, startTime, endTime, contextInfo.state, errors)
    if(Await.result(db.run(contexts.insertOrUpdate(row)), 60 seconds) == 0){
      throw new SlickException(s"Could not update ${contextInfo.id} in the database")
    }
  }

  override def getContextInfo(id: String): Future[Option[ContextInfo]] = {
    val query = contexts.filter(_.id === id).result
    db.run(query.headOption).map(r => r.map(contextInfoFromRow(_)))
  }

  override def getContextInfos(limit: Option[Int] = None, statusOpt: Option[String] = None):
  Future[Seq[ContextInfo]] = {
    val query = statusOpt match {
      case Some(i) => contexts.filter(_.state === i)
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

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = {
    val query = contexts.filter(_.name === name).sortBy(_.startTime.desc).result
    db.run(query.headOption).map(r => r.map(contextInfoFromRow(_)))
  }

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

  // Convert from joda DateTime to java.sql.Timestamp
  private def convertDateJodaToSql(dateTime: DateTime): Timestamp = new Timestamp(dateTime.getMillis)

  // Convert from java.sql.Timestamp to joda DateTime
  private def convertDateSqlToJoda(timestamp: Timestamp): DateTime = new DateTime(timestamp.getTime)

  override def getJobConfig(jobId: String): Future[Option[Config]] = {
    val query = configs
      .filter(_.jobId === jobId).map(_.jobConfig).result

    db.run(query.headOption).map(c => c.map(ConfigFactory.parseString(_)))
  }

  override def saveJobConfig(jobId: String, jobConfig: Config): Unit = {
    val configRender = jobConfig.root().render(ConfigRenderOptions.concise())
    if(Await.result(db.run(configs.map(c => c.*) += (jobId, configRender)), 60 seconds) == 0){
      throw new SlickException(s"Could not insert $jobId into database")
    }
  }

  override def saveJobInfo(jobInfo: JobInfo): Unit = {
    val jarId =
      Await.result(
        queryBinaryId(
          jobInfo.binaryInfo.appName,
          jobInfo.binaryInfo.binaryType,
          jobInfo.binaryInfo.uploadTime),
        60 seconds)
    val startTime = convertDateJodaToSql(jobInfo.startTime)
    val endTime = jobInfo.endTime.map(t => convertDateJodaToSql(t))
    val error = jobInfo.error.map(e => e.message)
    val errorClass = jobInfo.error.map(e => e.errorClass)
    val errorStackTrace = jobInfo.error.map(e => e.stackTrace)
    val row = (jobInfo.jobId, jobInfo.contextId, jobInfo.contextName, jarId, jobInfo.classPath,
      jobInfo.state, startTime, endTime, error, errorClass, errorStackTrace)
    if(Await.result(db.run(jobs.insertOrUpdate(row)), 60 seconds) == 0){
      throw new SlickException(s"Could not update ${jobInfo.jobId} in the database")
    }
  }

  private def jobInfoFromRow(row: (String, String, String, String, String,
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

  override def getJobInfos(limit: Int, statusOpt: Option[String] = None): Future[Seq[JobInfo]] = {

    val joinQuery = for {
      bin <- binaries
      j <- jobs if (statusOpt match {
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

  /**
    * Return all job ids to their job info.
    *
    * @return
    */
  override def getJobInfosByContextId(
      contextId: String, jobStatus: Option[String] = None): Future[Seq[JobInfo]] = {
    val joinQuery = for {
      bin <- binaries
      j <- jobs if ((contextId, jobStatus) match {
                          case (contextId, Some(jobStatus)) => j.binId === bin.binId &&
                              j.contextId === contextId && j.state === jobStatus
                          case _ => j.binId === bin.binId && j.contextId === contextId
                })
    } yield {
      (j.jobId, j.contextId, j.contextName, bin.appName, bin.binaryType, bin.uploadTime, j.classPath,
          j.state, j.startTime, j.endTime, j.error, j.errorClass, j.errorStackTrace)
    }
    db.run(joinQuery.result).map(_.map(jobInfoFromRow))
  }


  override def cleanRunningJobInfosForContext(contextId: String, endTime: DateTime): Future[Unit] = {
    val sqlEndTime = Some(convertDateJodaToSql(endTime))
    val error = Some(new ContextTerminatedException(contextId).getMessage())
    val selectQuery = for {
      j <- jobs if (j.contextId === contextId && j.state === JobStatus.Running)
    } yield (j.endTime, j.error, j.state)
    val updateQuery = selectQuery.update((sqlEndTime, error, JobStatus.Error))
    db.run(updateQuery).map(_ => ())
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = {

    // Join the BINARIES and JOBS tables without unnecessary columns
    val joinQuery = for {
      bin <- binaries
      j <- jobs if j.binId === bin.binId && j.jobId === jobId
    } yield {
      (j.jobId, j.contextId, j.contextName, bin.appName, bin.binaryType, bin.uploadTime, j.classPath,
         j.state, j.startTime, j.endTime, j.error, j.errorClass, j.errorStackTrace)
    }
    for (r <- db.run(joinQuery.result)) yield {
      r.map(jobInfoFromRow).headOption
    }
  }
}
