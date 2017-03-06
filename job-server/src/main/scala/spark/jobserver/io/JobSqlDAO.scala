package spark.jobserver.io

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import javax.sql.DataSource

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.commons.dbcp.BasicDataSource
import org.flywaydb.core.Flyway
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import slick.driver.JdbcProfile
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.runtime.universe

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
    def binary = column[Array[Byte]]("BINARY")
    def * = (binId, appName, binaryType, uploadTime, binary)
  }

  val binaries = TableQuery[Binaries]

  // Explicitly avoiding to label 'jarId' as a foreign key to avoid dealing with
  // referential integrity constraint violations.
  class Jobs(tag: Tag) extends Table[(String, String, Int, String, Timestamp,
    Option[Timestamp], Option[String])](tag, "JOBS") {
    def jobId = column[String]("JOB_ID", O.PrimaryKey)
    def contextName = column[String]("CONTEXT_NAME")
    def binId = column[Int]("BIN_ID")
    // FK to JARS table
    def classPath = column[String]("CLASSPATH")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Option[Timestamp]]("END_TIME")
    def error = column[Option[String]]("ERROR")
    def * = (jobId, contextName, binId, classPath, startTime, endTime, error)
  }

  val jobs = TableQuery[Jobs]

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
    // The order is important. Save the jar file first and then log it into database.
    cacheBinary(appName, binaryType, uploadTime, binBytes)

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
      throw new SlickException(s"Failed to delete binary: $appName from database")
    }
    cleanCacheBinaries(appName)
  }

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = {
    val query = binaries.groupBy { r =>
      (r.appName, r.binaryType)
    }.map {
      case ((appName, binaryType), bin) =>
        (appName, binaryType,  bin.map(_.uploadTime).max.get)
    }.result
    for (m <- db.run(query)) yield {
      m.map {
        case (appName, binaryType, uploadTime) =>
          (appName, (BinaryType.fromString(binaryType), convertDateSqlToJoda(uploadTime)))
      }.toMap
    }
  }

  // Insert JarInfo and its jar into db and return the primary key associated with that row
  private def insertBinaryInfo(binInfo: BinaryInfo, binBytes: Array[Byte]): Future[Int] = {
    db.run(binaries.map(j => j.*) += (
      -1,
      binInfo.appName,
      binInfo.binaryType.name,
      convertDateJodaToSql(binInfo.uploadTime),
      binBytes))
  }

  private def deleteBinaryInfo(appName: String): Future[Int] = {
    db.run(binaries.filter(_.appName === appName).delete)
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
    val query = binaries.filter { bin =>
      bin.appName === appName && bin.uploadTime === dateTime && bin.binaryType === binaryType.name
    }.map(_.binary).result
    db.run(query.head)
  }

  private def queryBinaryId(appName: String, binaryType: BinaryType, uploadTime: DateTime): Future[Int] = {
    val dateTime = convertDateJodaToSql(uploadTime)
    val query = binaries.filter { bin =>
      bin.appName === appName && bin.uploadTime === dateTime && bin.binaryType === binaryType.name
    }.map(_.binId).result
    db.run(query.head)
  }



  // Convert from joda DateTime to java.sql.Timestamp
  private def convertDateJodaToSql(dateTime: DateTime): Timestamp = new Timestamp(dateTime.getMillis)

  // Convert from java.sql.Timestamp to joda DateTime
  private def convertDateSqlToJoda(timestamp: Timestamp): DateTime = new DateTime(timestamp.getTime)

  override def getJobConfigs: Future[Map[String, Config]] = {
    for (r <- db.run(configs.result)) yield {
      r.map {
        case (jobId, jobConfig) => jobId -> ConfigFactory.parseString(jobConfig)
      }.toMap
    }
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
    val errors = jobInfo.error.map(e => e.getMessage)
    val row = (jobInfo.jobId, jobInfo.contextName, jarId, jobInfo.classPath, startTime, endTime, errors)
    if(Await.result(db.run(jobs.insertOrUpdate(row)), 60 seconds) == 0){
      throw new SlickException(s"Could not update ${jobInfo.jobId} in the database")
    }
  }

  override def getJobInfos(limit: Int, statusOpt: Option[String] = None): Future[Seq[JobInfo]] = {

    val joinQuery = for {
      bin <- binaries
      j <- jobs if j.binId === bin.binId && (statusOpt match {
                          // !endTime.isDefined
                          case Some(JobStatus.Running) => !j.endTime.isDefined && !j.error.isDefined
                          // endTime.isDefined && error.isDefined
                          case Some(JobStatus.Error) =>  j.error.isDefined
                          // not RUNNING AND NOT ERROR
                          case Some(JobStatus.Finished) => j.endTime.isDefined && !j.error.isDefined
                          case _ => true
                })
    } yield {
      (j.jobId, j.contextName, bin.appName, bin.binaryType,
        bin.uploadTime, j.classPath, j.startTime, j.endTime, j.error)
    }
    val sortQuery = joinQuery.sortBy(_._7.desc)
    val limitQuery = sortQuery.take(limit)
    // Transform the each row of the table into a map of JobInfo values
    for (r <- db.run(limitQuery.result)) yield {
      r.map { case (id, context, app, binType, upload, classpath, start, end, err) =>
        JobInfo(
          id,
          context,
          BinaryInfo(app, BinaryType.fromString(binType), convertDateSqlToJoda(upload)),
          classpath,
          convertDateSqlToJoda(start),
          end.map(convertDateSqlToJoda),
          err.map(new Throwable(_)))
      }
    }
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = {

    // Join the BINARIES and JOBS tables without unnecessary columns
    val joinQuery = for {
      bin <- binaries
      j <- jobs if j.binId === bin.binId && j.jobId === jobId
    } yield {
      (j.jobId, j.contextName, bin.appName, bin.binaryType, bin.uploadTime, j.classPath, j.startTime,
        j.endTime, j.error)
    }
    for (r <- db.run(joinQuery.result)) yield {
      r.map { case (id, context, app, binType, upload, classpath, start, end, err) =>
        JobInfo(id,
          context,
          BinaryInfo(app, BinaryType.fromString(binType), convertDateSqlToJoda(upload)),
          classpath,
          convertDateSqlToJoda(start),
          end.map(convertDateSqlToJoda),
          err.map(new Throwable(_)))
      }.headOption

    }
  }

  /**
    * Fetch submited jar or egg content for remote driver and JobManagerActor to cache in local
    *
    * @param appName
    * @param uploadTime
    * @return
    */
  override def getBinaryContent(appName: String, binaryType: BinaryType,
                                uploadTime: DateTime): Array[Byte] = {
    val jarFile = new File(rootDir, createBinaryName(appName, binaryType, uploadTime))
    if (!jarFile.exists()) {
      val binBytes = Await.result(fetchBinary(appName, binaryType, uploadTime), 60.seconds)
      cacheBinary(appName, binaryType, uploadTime, binBytes)
      binBytes
    } else {
      Files.readAllBytes(Paths.get(jarFile.getAbsolutePath))
    }
  }
}
