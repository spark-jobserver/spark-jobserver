package spark.jobserver.io

import java.io.File
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.UUID

import com.datastax.driver.core.querybuilder.{QueryBuilder => QB}
import com.datastax.driver.core.querybuilder.QueryBuilder._
import com.datastax.driver.core._
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction
import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.collection.convert.WrapAsJava
import scala.collection.convert.Wrappers.JListWrapper
import scala.concurrent.{Await, Future}
import spark.jobserver.cassandra.Cassandra.Resultset.toFuture

import scala.util.Try

object Metadata {
  val BinariesTable = "binaries"
  val BinariesChronologicalTable = "binaries_chronological"
  val BinaryId = "binary_id"
  val AppName = "app_name"
  val BType = "binary_type"
  val UploadTime = "upload_time"
  val ChunkIndex = "chunk_index"
  val Binary = "binary"

  val JobsTable = "jobs"
  val JobsChronologicalTable = "jobs_chronological"
  val JobId = "job_id"
  val ContextName = "context_name"
  val JobConfig = "job_config"
  val Classpath = "classpath"
  val StartTime = "start_time"
  val StartDate = "start_date"
  val EndTime = "end_time"
  val Error = "error"

}

class JobCassandraDAO(config: Config) extends JobDAO with FileCacher {

  import scala.concurrent.ExecutionContext.Implicits.global
  private val logger = LoggerFactory.getLogger(getClass)
  val session = setup(config)
  val chunkSizeInKb = Try(config.getInt("spark.jobserver.cassandra.chunk-size-in-kb")).getOrElse(1024)
  setupSchema()


  // NOTE: below is only needed for H2 drivers
  val rootDir = config.getString("spark.jobserver.sqldao.rootdir")
  val rootDirFile = new File(rootDir)
  logger.info("rootDir is " + rootDirFile.getAbsolutePath)
  initFileDirectory()

  override def saveBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          binBytes: Array[Byte]): Unit = {
    // The order is important. Save the binary file first and then log it into database.
    cacheBinary(appName, binaryType, uploadTime, binBytes)

    // log it into database
    val ok = insertBinaryInfo(BinaryInfo(appName, binaryType, uploadTime), binBytes)
    if (!ok) {
      logger.error(s"Fail save binary $appName, $uploadTime, content length ${binBytes.length}")
    }
  }

  import Metadata._

  /**
    * Delete a jar.
    *
    * @param appName
    */
  override def deleteBinary(appName: String): Unit = {
    Await.result(deleteBinaryInfo(appName), 60 seconds)
    cleanCacheBinaries(appName)
  }

  private def deleteBinaryInfo(appName: String): Future[Boolean] = {
    getApps.map { apps =>
      for ((name, (btype, upload)) <- apps) {
        session.execute(QB.delete().from(BinariesTable)
          .where(QB.eq(AppName, appName)).and(QB.eq(BType, btype.name))
          .and(QB.eq(UploadTime, upload.getMillis))
        )
        session.execute(QB.delete().from(BinariesChronologicalTable)
          .where(QB.eq(AppName, appName)).and(QB.eq(BType, btype.name))
        )
      }
      true
    }
  }

  private def insertBinaryInfo(binInfo: BinaryInfo, binBytes: Array[Byte]): Boolean = {
    session.executeAsync(insertInto(BinariesChronologicalTable).
      value(AppName, binInfo.appName).
      value(BType, binInfo.binaryType.name).
      value(UploadTime, binInfo.uploadTime.getMillis)
    ).getUninterruptibly().wasApplied()

    val bytesSize = 1024 * chunkSizeInKb
    val chunks = binBytes.sliding(bytesSize, bytesSize)
    try {
      val replies = chunks.zipWithIndex.map { case (chunk, index) =>
        session.executeAsync(insertInto(BinariesTable).
          value(AppName, binInfo.appName).
          value(BType, binInfo.binaryType.name).
          value(UploadTime, binInfo.uploadTime.getMillis).
          value(ChunkIndex, index)
          value(Binary, ByteBuffer.wrap(chunk))
        ).getUninterruptibly.wasApplied()
      }
      return replies.fold(true)(_ && _)
    } catch {
      case e: Throwable =>
        logger.error(s"Fail save chunk of file $binInfo", e)
        false
    }
  }

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = {

    implicit def ordering = new Ordering[DateTime] {
      override def compare(x: DateTime, y: DateTime): Int = java.lang.Long.compare(x.getMillis, y.getMillis)
    }

    val query = QB.select(AppName, BType, UploadTime).from(BinariesChronologicalTable)
    session.executeAsync(query).map { rs =>
      val apps = JListWrapper(rs.all()).map { row =>
        val appName = row.getString(AppName)
        val binaryType = BinaryType.fromString(row.getString(BType))
        val uploadTime = row.getTimestamp(UploadTime)
        appName -> (binaryType, new DateTime(uploadTime))
      }

      apps.sortBy { case (app, (binaryType, upload)) => -1 * upload.getMillis }.toMap
    }
  }

  override def saveJobConfig(jobId: String, jobConfig: Config): Unit = {
    session.executeAsync(
      insertInto(JobsTable).
        value(JobId, UUID.fromString(jobId)).
        value(JobConfig, jobConfig.root().render(ConfigRenderOptions.concise()))
    ).getUninterruptibly
  }

  override def getJobInfos(limit: Int, status: Option[String] = None): Future[Seq[JobInfo]] = {
    import Metadata._
    val query = QB.select(
      JobId, ContextName, AppName, BType, UploadTime, Classpath, StartTime, EndTime, Error
    ).from(JobsChronologicalTable).where(QB.eq(StartDate, today())).limit(limit)

    session.executeAsync(query).map { rs =>
      val allJobs = JListWrapper(rs.all()).map { row =>
        val endTime = row.getTimestamp(EndTime)
        val error = row.getString(Error)

        JobInfo(
          row.getUUID(JobId).toString,
          row.getString(ContextName),
          BinaryInfo(
            row.getString(AppName),
            BinaryType.fromString(row.getString(BType)),
            new DateTime(row.getTimestamp(UploadTime))
          ),
          row.getString(Classpath),
          new DateTime(row.getTimestamp(StartTime)),
          if (endTime != null) Option(new DateTime(endTime)) else None,
          if (error != null) Option(new Throwable(error)) else None
        )
      }
      status match {
        // !endTime.isDefined
        case Some(JobStatus.Running) => allJobs.filter(j => j.endTime.isEmpty && j.error.isEmpty)
        // endTime.isDefined && error.isDefined
        case Some(JobStatus.Error) => allJobs.filter(j => j.error.isDefined)
        // not RUNNING AND NOT ERROR
        case Some(JobStatus.Finished) => allJobs.filter(j => j.endTime.isDefined && j.error.isEmpty)
        case _ => allJobs
      }
    }
  }

  override def retrieveBinaryFile(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    val binaryFile = new File(rootDir, createBinaryName(appName, binaryType, uploadTime))
    if (!binaryFile.exists()) {
      fetchAndCacheBinaryFile(appName, binaryType, uploadTime)
    }
    binaryFile.getAbsolutePath
  }

  private def today(): LocalDate = {
    LocalDate.fromMillisSinceEpoch(DateTime.now.getMillis)
  }

  // Fetch the binary file from database and cache it into local file system.
  private def fetchAndCacheBinaryFile(appName: String, binaryType: BinaryType, uploadTime: DateTime) {
    val binBytes = fetchBinary(appName, binaryType, uploadTime)
    cacheBinary(appName, binaryType, uploadTime, binBytes)
  }

  // Fetch the binary from the database
  private def fetchBinary(appName: String, binaryType: BinaryType, uploadTime: DateTime): Array[Byte] = {
    val rows = session.executeAsync(QB.select(AppName, BType, UploadTime, ChunkIndex, Binary).
      from(BinariesTable).
      where(QB.eq(AppName, appName)).
      and(QB.eq(BType, binaryType.name)).
      and(QB.eq(UploadTime, uploadTime.getMillis))
    ).getUninterruptibly().all()

    val tuples = JListWrapper(rows).toIndexedSeq.map { row =>
      (row.getInt(ChunkIndex), row.getBytes(Binary).array())
    }
    tuples.sortBy(_._1).toMap.values.foldLeft(Array[Byte]()) { _ ++ _ }
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = {
    val query = QB.select(
      JobId, ContextName, AppName, BType, UploadTime, Classpath, StartTime, EndTime, Error
    ).from(JobsTable).
      where(QB.eq(JobId, UUID.fromString(jobId))).
      limit(1)

    session.executeAsync(query).map { rs =>
      val row = rs.one()
      Option(row).map(r =>
        JobInfo(
          r.getUUID(Metadata.JobId).toString,
          r.getString(ContextName),
          BinaryInfo(
            r.getString(AppName),
            BinaryType.fromString(r.getString(BType)),
            new DateTime(r.getTimestamp(UploadTime))
          ),
          r.getString(Classpath),
          new DateTime(r.getTimestamp(StartTime)),
          Option(r.getTimestamp(EndTime)).map(new DateTime(_)),
          Option(r.getString(Error)).map(new Throwable(_))
        ))
    }
  }

  override def saveJobInfo(jobInfo: JobInfo): Unit = {
    val JobInfo(jobId, contextName, binaryInfo, classPath, startTime, endTime, error) = jobInfo
    val (_, endOpt, errOpt) = (startTime,
      endTime.map(e => e),
      error.map(_.getMessage))

    val localDate: LocalDate = LocalDate.fromMillisSinceEpoch(jobInfo.startTime.getMillis)

    val insert = insertInto(JobsTable).
      value(JobId, UUID.fromString(jobId)).
      value(ContextName, contextName).
      value(AppName, binaryInfo.appName).
      value(BType, binaryInfo.binaryType.name).
      value(UploadTime, binaryInfo.uploadTime.getMillis).
      value(Classpath, classPath).
      value(StartTime, startTime.getMillis).
      value(StartDate, localDate)

    endOpt.foreach{e => insert.value(EndTime, e.getMillis)}
    errOpt.foreach(insert.value(Error, _))
    session.execute(insert)

    val insert2 = insertInto(JobsChronologicalTable).
      value(StartDate, localDate).
      value(StartTime, startTime.getMillis).
      value(JobId, UUID.fromString(jobId)).
      value(ContextName, contextName).
      value(AppName, binaryInfo.appName).
      value(BType, binaryInfo.binaryType.name).
      value(UploadTime, binaryInfo.uploadTime.getMillis).
      value(Classpath, classPath)

    endOpt.foreach{e => insert2.value(EndTime, e.getMillis)}
    errOpt.foreach(insert2.value(Error, _))
    session.execute(insert2)
  }

  override def getJobConfigs: Future[Map[String, Config]] = {
    val query = QB.select(Metadata.JobId, Metadata.JobConfig).from(Metadata.JobsTable)
    session.executeAsync(query).map { rs =>
      JListWrapper(rs.all()).map { row =>
        val config = Option(row.getString(Metadata.JobConfig)).getOrElse("")
        (row.getUUID(Metadata.JobId).toString, ConfigFactory.parseString(config))
      }.toMap
    }
  }

  private def setup(config: Config): Session = {
    val cassandraConfig = config.getConfig("spark.jobserver.cassandra")
    val hosts = JListWrapper(cassandraConfig.getStringList("hosts"))
    val username = cassandraConfig.getString("user")
    val password = cassandraConfig.getString("password")
    val consistencyLevel = Try(
      ConsistencyLevel.valueOf(cassandraConfig.getString("consistency"))
    ).getOrElse(ConsistencyLevel.ONE)
    val keyspace = "spark_jobserver"
    val addrs = hosts.map(_.trim()).map(input => {
      var port: Int = 9042
      var host: String = input
      val idx: Int = host.indexOf(":")
      if (idx != -1) {
        port = host.substring(idx + 1).toInt
        host = host.substring(0, idx)
      }
      new InetSocketAddress(host, port)
    })
    val queryOptions = new QueryOptions().setConsistencyLevel(consistencyLevel)
    val cluster = Cluster.builder
      .addContactPointsWithPorts(WrapAsJava.asJavaCollection(addrs))
      .withQueryOptions(queryOptions)
      .withCredentials(username, password)
      .build
    cluster.getConfiguration.getProtocolOptions.setCompression(ProtocolOptions.Compression.LZ4)
    cluster.connect(keyspace)
  }

  private def setupSchema() = {
    import Metadata._

    val binariesTable: Create = SchemaBuilder.createTable(BinariesTable).ifNotExists.
      addPartitionKey(AppName, DataType.text).
      addPartitionKey(BType, DataType.text).
      addPartitionKey(UploadTime, DataType.timestamp).
      addClusteringColumn(ChunkIndex, DataType.cint()).
      addColumn(Binary, DataType.blob)

    session.execute(binariesTable)

    val binariesChronologicalTable: Create = SchemaBuilder.createTable(BinariesChronologicalTable).
      ifNotExists().
      addPartitionKey(AppName, DataType.text).
      addPartitionKey(BType, DataType.text).
      addColumn(UploadTime, DataType.timestamp())

    session.execute(binariesChronologicalTable)

    val jobsTableStatement = SchemaBuilder.createTable(JobsTable).ifNotExists.
      addPartitionKey(JobId, DataType.uuid).
      addColumn(ContextName, DataType.text).
      addColumn(AppName, DataType.text).
      addColumn(BType, DataType.text).
      addColumn(UploadTime, DataType.timestamp).
      addColumn(JobConfig, DataType.text).
      addColumn(Classpath, DataType.text).
      addColumn(StartTime, DataType.timestamp).
      addColumn(StartDate, DataType.date).
      addColumn(EndTime, DataType.timestamp).
      addColumn(Error, DataType.text)

    session.execute(jobsTableStatement)

    val jobsChronologicalView = SchemaBuilder.createTable(JobsChronologicalTable).ifNotExists().
      addPartitionKey(StartDate, DataType.date).
      addClusteringColumn(StartTime, DataType.timestamp()).
      addClusteringColumn(JobId, DataType.uuid()).
      addColumn(ContextName, DataType.text).
      addColumn(AppName, DataType.text).
      addColumn(BType, DataType.text).
      addColumn(UploadTime, DataType.timestamp).
      addColumn(JobConfig, DataType.text).
      addColumn(Classpath, DataType.text).
      addColumn(EndTime, DataType.timestamp).
      addColumn(Error, DataType.text).
      withOptions().clusteringOrder(StartTime, Direction.DESC)

    session.execute(jobsChronologicalView)

  }

  override def getBinaryContent(appName: String, binaryType: BinaryType,
                                uploadTime: DateTime): Array[Byte] = {
    val jarFile = new File(rootDir, createBinaryName(appName, binaryType, uploadTime))
    if (!jarFile.exists()) {
      val binBytes = fetchBinary(appName, binaryType, uploadTime)
      cacheBinary(appName, binaryType, uploadTime, binBytes)
      binBytes
    } else {
      Files.readAllBytes(Paths.get(jarFile.getAbsolutePath))
    }
  }
}
