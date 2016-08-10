package spark.jobserver.io

import java.io.{BufferedOutputStream, File, FileOutputStream}
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

import scala.reflect.runtime.universe
import slick.driver.JdbcProfile

class JobSqlDAO(config: Config) extends JobDAO {
  val slickDriverClass = config.getString("spark.jobserver.sqldao.slick-driver")
  val jdbcDriverClass = config.getString("spark.jobserver.sqldao.jdbc-driver")

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profileModule = runtimeMirror.staticModule(slickDriverClass)
  val profile = runtimeMirror.reflectModule(profileModule).instance.asInstanceOf[JdbcProfile]

  import profile.api._

  private val logger = LoggerFactory.getLogger(getClass)

  // NOTE: below is only needed for H2 drivers
  private val rootDir = config.getString("spark.jobserver.sqldao.rootdir")
  private val rootDirFile = new File(rootDir)
  logger.info("rootDir is " + rootDirFile.getAbsolutePath)

  // Definition of the tables
  //scalastyle:off
  class Jars(tag: Tag) extends Table[(Int, String, Timestamp, Array[Byte])](tag, "JARS") {
    def jarId = column[Int]("JAR_ID", O.PrimaryKey, O.AutoInc)
    def appName = column[String]("APP_NAME")
    def uploadTime = column[Timestamp]("UPLOAD_TIME")
    def jar = column[Array[Byte]]("JAR")
    def * = (jarId, appName, uploadTime, jar)
  }

  val jars = TableQuery[Jars]

  // Explicitly avoiding to label 'jarId' as a foreign key to avoid dealing with
  // referential integrity constraint violations.
  class Jobs(tag: Tag) extends Table[(String, String, Int, String, Timestamp,
    Option[Timestamp], Option[String])](tag, "JOBS") {
    def jobId = column[String]("JOB_ID", O.PrimaryKey)
    def contextName = column[String]("CONTEXT_NAME")
    def jarId = column[Int]("JAR_ID")
    // FK to JARS table
    def classPath = column[String]("CLASSPATH")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Option[Timestamp]]("END_TIME")
    def error = column[Option[String]]("ERROR")
    def * = (jobId, contextName, jarId, classPath, startTime, endTime, error)
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

  // Server initialization
  init()

  private def init() {
    // Create the data directory if it doesn't exist
    if (!rootDirFile.exists()) {
      if (!rootDirFile.mkdirs()) {
        throw new RuntimeException("Could not create directory " + rootDir)
      }
    }

    // Flyway migration
    val flyway = new Flyway()
    flyway.setDataSource(jdbcUrl, jdbcUser, jdbcPassword)
    // TODO: flyway.setLocations(migrateLocations) should be removed when tests have a running configuration
    flyway.setLocations(migrateLocations)
    flyway.migrate()
  }

  override def saveJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]) {
    // The order is important. Save the jar file first and then log it into database.
    cacheJar(appName, uploadTime, jarBytes)

    // log it into database
    if (Await.result(insertJarInfo(JarInfo(appName, uploadTime), jarBytes), 60 seconds) == 0) {
      throw new SlickException(s"Failed to insert jar: $appName into database at $uploadTime")
    }
  }

  override def getApps: Future[Map[String, DateTime]] = {
    val query = jars.groupBy {
      _.appName
    }.map {
      case (appName, jar) => (appName, jar.map(_.uploadTime).max.get)
    }.result
    for (m <- db.run(query)) yield {
      m.map {
        case (appName, jar) => (appName, convertDateSqlToJoda(jar))
      }.toMap
    }
  }

  // Insert JarInfo and its jar into db and return the primary key associated with that row
  private def insertJarInfo(jarInfo: JarInfo, jarBytes: Array[Byte]): Future[Int] = {
    db.run(jars.map(j => j.*) += (-1, jarInfo.appName, convertDateJodaToSql(jarInfo.uploadTime), jarBytes))
  }

  override def retrieveJarFile(appName: String, uploadTime: DateTime): String = {
    val jarFile = new File(rootDir, createJarName(appName, uploadTime) + ".jar")
    if (!jarFile.exists()) {
      fetchAndCacheJarFile(appName, uploadTime)
    }
    jarFile.getAbsolutePath
  }

  // Fetch the jar file from database and cache it into local file system.
  private def fetchAndCacheJarFile(appName: String, uploadTime: DateTime) {
    val jarBytes = Await.result(fetchJar(appName, uploadTime), 60 seconds)
    cacheJar(appName, uploadTime, jarBytes)
  }

  // Fetch the jar from the database
  private def fetchJar(appName: String, uploadTime: DateTime): Future[Array[Byte]] = {
    val dateTime = convertDateJodaToSql(uploadTime)
    val query = jars.filter { jar =>
      jar.appName === appName && jar.uploadTime === dateTime
    }.map(_.jar).result
    db.run(query.head)
  }

  private def queryJarId(appName: String, uploadTime: DateTime): Future[Int] = {
    val dateTime = convertDateJodaToSql(uploadTime)
    val query = jars.filter { jar =>
      jar.appName === appName && jar.uploadTime === dateTime
    }.map(_.jarId).result
    db.run(query.head)
  }

  // Cache the jar file into local file system.
  private def cacheJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]) {
    val outFile = new File(rootDir, createJarName(appName, uploadTime) + ".jar")
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", jarBytes.length, outFile.getPath)
      bos.write(jarBytes)
      bos.flush()
    } finally {
      bos.close()
    }
  }

  private def createJarName(appName: String, uploadTime: DateTime): String =
    appName + "-" + uploadTime.toString("yyyyMMdd_hhmmss_SSS")

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
    val jarId = Await.result(queryJarId(jobInfo.jarInfo.appName, jobInfo.jarInfo.uploadTime), 60 seconds)
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
      jar <- jars
      j <- jobs if j.jarId === jar.jarId && (statusOpt match {
                          // !endTime.isDefined
                          case Some(JobStatus.Running) => !j.endTime.isDefined && !j.error.isDefined
                          // endTime.isDefined && error.isDefined
                          case Some(JobStatus.Error) =>  j.error.isDefined
                          // not RUNNING AND NOT ERROR
                          case Some(JobStatus.Finished) => j.endTime.isDefined && !j.error.isDefined
                          case _ => true
                })
    } yield {
      (j.jobId, j.contextName, jar.appName, jar.uploadTime, j.classPath, j.startTime, j.endTime, j.error)
    }
    val sortQuery = joinQuery.sortBy(_._6.desc)
    val limitQuery = sortQuery.take(limit)
    // Transform the each row of the table into a map of JobInfo values
    for (r <- db.run(limitQuery.result)) yield {
      r.map { case (id, context, app, upload, classpath, start, end, err) =>
        JobInfo(id, context, JarInfo(app, convertDateSqlToJoda(upload)), classpath,
          convertDateSqlToJoda(start),
          end.map(convertDateSqlToJoda(_)),
          err.map(new Throwable(_)))
      }
    }
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = {

    // Join the JARS and JOBS tables without unnecessary columns
    val joinQuery = for {
      jar <- jars
      j <- jobs if j.jarId === jar.jarId && j.jobId === jobId
    } yield {
      (j.jobId, j.contextName, jar.appName, jar.uploadTime, j.classPath, j.startTime,
        j.endTime, j.error)
    }
    for (r <- db.run(joinQuery.result)) yield {
      r.map { case (id, context, app, upload, classpath, start, end, err) =>
        JobInfo(id,
          context,
          JarInfo(app, convertDateSqlToJoda(upload)),
          classpath,
          convertDateSqlToJoda(start),
          end.map(convertDateSqlToJoda(_)),
          err.map(new Throwable(_)))
      }.headOption

    }
  }
}
