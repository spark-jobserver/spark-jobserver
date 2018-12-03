package spark.jobserver.io

import java.io.File
import java.sql.{Blob, Timestamp}
import javax.sql.rowset.serial.SerialBlob

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.reflect.runtime.universe

import com.typesafe.config.Config
import org.flywaydb.core.Flyway
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import slick.driver.JdbcProfile
import spark.jobserver.JobManagerActor.ContextTerminatedException
import spark.jobserver.util.NoSuchBinaryException

/**
  * Multiple threads can access the functions in this class at the same.
  * Don't use mutable objects as it will compromise thread safety.
  * @param config config of jobserver
  */
class JobSqlDAO(config: Config) extends JobDAO with FileCacher {
  private val logger = LoggerFactory.getLogger(getClass)

  val sqlCommon: SqlCommon = new SqlCommon(config)

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profile = runtimeMirror.reflectModule(sqlCommon.profileModule).instance.asInstanceOf[JdbcProfile]

  import profile.api._

  // NOTE: below is only needed for H2 drivers
  override val rootDir = config.getString("spark.jobserver.sqldao.rootdir")
  override val rootDirFile = new File(rootDir)
  logger.info("rootDir is " + rootDirFile.getAbsolutePath)

  // Definition of the tables
  //scalastyle:off
  class BinariesContents(tag: Tag) extends Table[(Array[Byte], Blob)](tag, "BINARIES_CONTENTS") {
    def binHash = column[Array[Byte]]("BIN_HASH", O.PrimaryKey)
    def binary = column[Blob]("BINARY")
    def * = (binHash, binary)
  }

  val binariesContents = TableQuery[BinariesContents]
  //scalastyle:on

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
    flyway.setDataSource(sqlCommon.jdbcUrl, sqlCommon.jdbcUser, sqlCommon.jdbcPassword)
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
    val query = sqlCommon.binaries.groupBy { r =>
      (r.appName, r.binaryType)
    }.map {
      case ((appName, binaryType), bin) =>
        (appName, binaryType, bin.map(_.uploadTime).max.get)
    }.result
    for (m <- sqlCommon.db.run(query)) yield {
      m.map {
        case (appName, binaryType, uploadTime) =>
          (appName, (BinaryType.fromString(binaryType), sqlCommon.convertDateSqlToJoda(uploadTime)))
      }.toMap
    }
  }

  override def getLastUploadTimeAndType(appName: String): Option[(DateTime, BinaryType)] = {
    val query = sqlCommon.binaries.filter(_.appName === appName)
      .sortBy(_.uploadTime.desc)
      .map(b => (b.uploadTime, b.binaryType)).result
      .map{_.headOption.map(b => (sqlCommon.convertDateSqlToJoda(b._1), BinaryType.fromString(b._2)))}
    Await.result(sqlCommon.db.run(query), 60 seconds)
  }

  // Insert JarInfo and its jar into db and return the primary key associated with that row
  private def insertBinaryInfo(binInfo: BinaryInfo, binBytes: Array[Byte]): Future[Int] = {
    val hash = BinaryDAO.calculateBinaryHash(binBytes);
    val dbAction = (sqlCommon.binaries +=
        (-1, binInfo.appName, binInfo.binaryType.name,
            sqlCommon.convertDateJodaToSql(binInfo.uploadTime), hash))
                     .andThen(binariesContents.filter(_.binHash === hash).map(_.binHash)
                         .result.headOption.flatMap {
                       case Some(bc) => DBIO.successful(1)
                       case None => binariesContents += (hash, new SerialBlob(binBytes))
                     }).transactionally
    sqlCommon.db.run(dbAction)
  }

  private def logDeleteErrors = PartialFunction[Any, Int] {
    case e: Throwable => logger.error(e.getMessage, e); 0
    case c: Int => c
  }

  private def deleteBinaryInfo(appName: String): Future[Int] = {
    val deleteBinary = sqlCommon.binaries.filter(_.appName === appName)
    val hashUsed = sqlCommon.binaries
    .filter(_.binHash in deleteBinary.map(_.binHash)).filter(_.appName =!= appName)
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
    sqlCommon.db.run(dbAction).recover(logDeleteErrors)
  }

  // Fetch the binary file from the database
  private def getBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime): Future[Array[Byte]] = {
    val dateTime = sqlCommon.convertDateJodaToSql(uploadTime)
    val query = for {
      b <- sqlCommon.binaries.filter { bin =>
        bin.appName === appName && bin.uploadTime === dateTime && bin.binaryType === binaryType.name
      }
      bc <- binariesContents if b.binHash === bc.binHash
    } yield bc.binary
    val dbAction = query.result
    sqlCommon.db.run(dbAction.head.map { b => b.getBytes(1, b.length.toInt) }.transactionally)
  }

  override def getBinaryFilePath(appName: String,
                                 binaryType: BinaryType,
                                 uploadTime: DateTime): String = {
    getPath(appName, binaryType, uploadTime) match {
      case Some(path) => path
      case None =>
        val binBytes = Await.result(getBinary(appName, binaryType, uploadTime), 60 seconds)
        cacheBinary(appName, binaryType, uploadTime, binBytes)
    }
  }

  override def saveContextInfo(contextInfo: ContextInfo): Unit = {
    sqlCommon.saveContext(contextInfo)
  }

  override def getContextInfo(id: String): Future[Option[ContextInfo]] = {
    sqlCommon.getContext(id)
  }

  override def getContextInfos(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
  Future[Seq[ContextInfo]] = {
    sqlCommon.getContexts(limit, statuses)
  }

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = {
    sqlCommon.getContextByName(name)
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = {
    sqlCommon.getJobConfig(jobId)
  }

  override def saveJobConfig(jobId: String, jobConfig: Config): Unit = {
    sqlCommon.saveJobConfig(jobId, jobConfig)
  }

  override def saveJobInfo(jobInfo: JobInfo): Unit = {
    val jarId =
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
    val row = (jobInfo.jobId, jobInfo.contextId, jobInfo.contextName, jarId, jobInfo.classPath,
      jobInfo.state, startTime, endTime, error, errorClass, errorStackTrace)
    if (Await.result(sqlCommon.db.run(sqlCommon.jobs.insertOrUpdate(row)), 60 seconds) == 0) {
      throw new SlickException(s"Could not update ${jobInfo.jobId} in the database")
    }
  }

  override def getJobInfos(limit: Int, statusOpt: Option[String] = None): Future[Seq[JobInfo]] = {
    sqlCommon.getJobs(limit, statusOpt)
  }

  /**
    * Return all job ids to their job info.
    *
    * @return
    */
  override def getJobInfosByContextId(
      contextId: String, jobStatuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = {
    sqlCommon.getJobsByContextId(contextId, jobStatuses)
  }


  override def cleanRunningJobInfosForContext(contextId: String, endTime: DateTime): Future[Unit] = {
    val sqlEndTime = Some(sqlCommon.convertDateJodaToSql(endTime))
    val error = Some(ContextTerminatedException(contextId).getMessage())
    val selectQuery = for {
      j <- sqlCommon.jobs if (j.contextId === contextId && j.state === JobStatus.Running)
    } yield (j.endTime, j.error, j.state)
    val updateQuery = selectQuery.update((sqlEndTime, error, JobStatus.Error))
    sqlCommon.db.run(updateQuery).map(_ => ())
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = {
    sqlCommon.getJob(jobId)
  }

  /**
    * Generatd SQL Query
    * select "BIN_HASH" from "BINARIES_CONTENTS"
    * where x4 = next
    * @return Sequence of hashes
    */
  override def getAllHashes(): Future[Seq[String]] = {
    val query = binariesContents.map(bc => bc.binHash)
    for (hashes <- sqlCommon.db.run(query.result)) yield {
      hashes.map(BinaryDAO.hashBytesToString(_))
    }
  }

  override def getBinaryBytes(hash: String): Future[Array[Byte]] = {
    if (hash.isEmpty) {
      return Future { Array.emptyByteArray }
    }

    val query = for {
      bc <- binariesContents.filter(_.binHash === BinaryDAO.hashStringToBytes(hash))
    } yield bc.binary
    sqlCommon.db.run(query.result.headOption.map{
      b =>
        b match {
          case Some(byte) => byte.getBytes(1, byte.length().toInt)
          case None => Array.emptyByteArray
        }
    }.transactionally)
  }

  /**
    * select "BIN_HASH"
    * from "BINARIES"
    * where
    *   "BIN_HASH" in
    *     (select distinct "BIN_HASH" from "BINARIES" where "APP_NAME" = 'abc')
    * @param appName
    * @return
    */
  override def getHashForApp(appName: String): Future[Seq[String]] = {
    val allHashesForApp = sqlCommon.binaries.filter(_.appName === appName).map(_.binHash).distinct
    val allBinariesUsingHash = sqlCommon.binaries.filter(_.binHash in allHashesForApp)

    // Convert hash bytes to string
    for (hashes <- sqlCommon.db.run(allBinariesUsingHash.result)) yield {
      val appNameAndBytes = hashes.map {
        case (_, appName, _, _, hashBytes) => (appName, BinaryDAO.hashBytesToString(hashBytes))
      }

      // Find all apps using hash whose name is not "appName"
      val otherAppsUsingThisHash = appNameAndBytes.filter(_._1 != appName)
      otherAppsUsingThisHash.length match {
        case 0 =>
          // If no other is using then return all hashes against appName
          appNameAndBytes.map(_._2).distinct
        case _ => Seq()
      }
    }
  }
}
