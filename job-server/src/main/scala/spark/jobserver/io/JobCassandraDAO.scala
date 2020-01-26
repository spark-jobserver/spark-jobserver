package spark.jobserver.io

import java.io.File
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.UUID

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder._
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder => QB}
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction
import com.datastax.driver.core.schemabuilder.{Create, CreateType, SchemaBuilder}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.cassandra.Cassandra.Resultset.toFuture

import scala.collection.JavaConversions
import scala.collection.JavaConverters._
import scala.collection.convert.WrapAsJava
import scala.collection.convert.Wrappers.JListWrapper
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Try

object Metadata {
  val keyspace = "spark_jobserver"

  val BinariesTable = "binaries"
  val BinariesChronologicalTable = "binaries_chronological"
  val BinaryId = "binary_id"
  val AppName = "app_name"
  val BType = "binary_type"
  val UploadTime = "upload_time"
  val ChunkIndex = "chunk_index"
  val Binary = "binary"

  val ContextsTable = "contexts"
  val OrderedContextsByNameTable = "contexts_chronological_name"
  val OrderedContextsByStateTable = "contexts_chronological_state"
  val ContextId = "id"
  val ContextConfig = "config"
  val State = "state"
  val ActorAddress = "actor_address"

  val JobsTable = "jobs"
  val JobsChronologicalTable = "jobs_chronological"
  val JobsByContextIdTable = "jobs_context_id"
  val JobId = "job_id"
  val ContextName = "context_name"
  val JobConfig = "job_config"
  val Classpath = "classpath"
  val StartTime = "start_time"
  val StartDate = "start_date"
  val EndTime = "end_time"
  val Error = "error"
  val ErrorClass = "error_class"
  val ErrorStackTrace = "error_stack_trace"
  val Cp = "cp"

  val BinInfo = "binaryInfo"
}

@Deprecated
class JobCassandraDAO(config: Config) extends JobDAO with FileCacher {

  private val logger = LoggerFactory.getLogger(getClass)
  private val session = setup(config)
  private val chunkSizeInKb = Try(config.getInt("spark.jobserver.cassandra.chunk-size-in-kb")).getOrElse(1024)
  setupSchema()

  override val rootDir = config.getString("spark.jobserver.cassandradao.rootdir")
  override val rootDirFile = new File(rootDir)
  logger.info("File caching rootDir is " + rootDirFile.getAbsolutePath)
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

  override def saveContextInfo(contextInfo: ContextInfo): Unit = {
    def fillInsert(insert: Insert): Insert = {
      insert.
        value(ContextId, UUID.fromString(contextInfo.id)).
        value(ContextName, contextInfo.name).
        value(ContextConfig, contextInfo.config).
        value(State, contextInfo.state).
        value(StartTime, contextInfo.startTime.getMillis)
        contextInfo.actorAddress.foreach { address => insert.value(ActorAddress, address) }
        contextInfo.endTime.foreach{ endTime => insert.value(EndTime, endTime.getMillis) }
        contextInfo.error.foreach { err =>
          insert.value(Error, err.getMessage)
        }
      insert
    }

    session.execute(fillInsert(insertInto(ContextsTable)))
    session.execute(fillInsert(insertInto(OrderedContextsByNameTable)))
    session.execute(fillInsert(insertInto(OrderedContextsByStateTable)))
  }

  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
      Future[Seq[JobInfo]] = {
    throw new NotImplementedError()
  }

  override def getContextInfo(id: String): Future[Option[ContextInfo]] = {
    val query = QB.select(
        ContextId, ContextName, ContextConfig, ActorAddress, StartTime, EndTime,
        State, Error).
      from(ContextsTable).
      where(QB.eq(ContextId, UUID.fromString(id))).
      limit(1)

    session.executeAsync(query).map { rs =>
      val row = rs.one()
      Option(row).map(rowToContextInfo)
    }
  }

  override def getContextInfos(limitOpt: Option[Int] = None, statuses: Option[Seq[String]] = None):
    Future[Seq[ContextInfo]] = {
    val query = QB.select(ContextId, ContextName, ContextConfig, ActorAddress, StartTime, EndTime,
            State, Error).
        from(OrderedContextsByStateTable)
    val filteredQuery = (limitOpt, statuses) match {
       case (Some(limit), Some(statuses)) => query.where(QB.in(State, statuses.toList.asJava)).limit(limit)
       case (Some(limit), None) =>
         throw new UnsupportedOperationException("Current cassandra model doesnot support this operation")
       case (None, Some(statuses)) => query.where(QB.in(State, statuses.toList.asJava))
       case (None, None) =>
         throw new UnsupportedOperationException("Current cassandra model doesnot support this operation")
     }

    session.executeAsync(filteredQuery).map { rs =>
       JListWrapper(rs.all()).map(rowToContextInfo)
    }
  }

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = {
    val query = QB.select(ContextId, ContextName, ContextConfig, ActorAddress, StartTime, EndTime,
        State, Error).
    from(OrderedContextsByNameTable).
    where(QB.eq(ContextName, name))

    session.executeAsync(query).map { rs =>
      val allContexts = JListWrapper(rs.all()).map(rowToContextInfo)
      allContexts.filter(_.name == name).headOption
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
    val query = QB.select(
      JobId, ContextId, ContextName, AppName, BType, UploadTime, Classpath, State, StartTime, EndTime,
      Error, ErrorClass, ErrorStackTrace, Cp
    ).from(JobsChronologicalTable).where(QB.eq(StartDate, today())).limit(limit)

    session.executeAsync(query).map { rs =>
      val allJobs = JListWrapper(rs.all()).map(rowToJobInfo)
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

  override def getJobInfosByContextId(
      contextId: String, jobStatuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = {
    var query = QB.select(
      JobId, ContextId, ContextName, AppName, BType, UploadTime, Classpath, State, StartTime, EndTime,
      Error, ErrorClass, ErrorStackTrace, Cp
    ).from(JobsByContextIdTable).where(QB.eq(ContextId, UUID.fromString(contextId)))

    query = jobStatuses match {
      case Some(statuses) => query.and(QB.in(State, statuses.toList.asJava))
      case _ => query
    }
    session.executeAsync(query).map { rs =>
      JListWrapper(rs.all()).map(rowToJobInfo)
    }
  }

  private def today(): LocalDate = {
    LocalDate.fromMillisSinceEpoch(DateTime.now.getMillis)
  }

  // Fetch the binary from the database
  private def getBinary(appName: String,
                         binaryType: BinaryType,
                         uploadTime: DateTime): Array[Byte] = {
    val rows = session.executeAsync(QB.select(AppName, BType, UploadTime, ChunkIndex, Binary).
      from(BinariesTable).
      where(QB.eq(AppName, appName)).
      and(QB.eq(BType, binaryType.name)).
      and(QB.eq(UploadTime, uploadTime.getMillis))
    ).getUninterruptibly().all()

    val tuples = JListWrapper(rows).toIndexedSeq.map { row =>
      (row.getInt(ChunkIndex), row.getBytes(Binary).array())
    }
    tuples.map(_._2).foldLeft(Array[Byte]()) { _ ++ _ }
  }

  override def getBinaryFilePath(appName: String,
                                 binaryType: BinaryType,
                                 uploadTime: DateTime): String = {
    getPath(appName, binaryType, uploadTime) match {
      case Some(path) => path
      case None =>
        val binBytes = getBinary(appName, binaryType, uploadTime)
        cacheBinary(appName, binaryType, uploadTime, binBytes)
    }
  }

  private def rowToContextInfo(row: Row): ContextInfo = {
    ContextInfo(
      row.getUUID(ContextId).toString,
      row.getString(ContextName),
      row.getString(ContextConfig),
      Option(row.getString(ActorAddress)),
      new DateTime(row.getTimestamp(StartTime)),
      Option(row.getTimestamp(EndTime)).map(new DateTime(_)),
      row.getString(State),
      Option(row.getString("Error")).map(new Exception(_))
    )
  }

  private def rowToJobInfo(row: Row): JobInfo = {
    val errorData = Option(row.getString(Error)).map { error =>
      val errorClass = if (row.isNull(ErrorClass)) "" else row.getString(ErrorClass)
      val stackTrace = if (row.isNull(ErrorStackTrace)) "" else row.getString(ErrorStackTrace)
      ErrorData(error, errorClass, stackTrace)
    }
    val cpBinaryInfos = if (row.isNull(Cp)) Seq.empty else {
      val cpRow = row.getSet(Cp, classOf[UDTValue])
      cpRow.asScala.map(r => BinaryInfo(
        r.getString(AppName),
        BinaryType.fromString(r.getString(BType)),
        new DateTime(r.getTime(UploadTime))
      )).toSeq
    }
    // Binary info for Jobs was moved to cp value (and Cassandra tables were changed accordingly).
    // This is compatability code, which takes into account data written before the migration.
    val legacyBinInfoData = if (row.isNull(AppName)) Seq.empty else {
      Seq(BinaryInfo(
        row.getString(AppName),
        BinaryType.fromString(row.getString(BType)),
        new DateTime(row.getTimestamp(UploadTime))
      ))
    }
    JobInfo(
      row.getUUID(Metadata.JobId).toString,
      row.getUUID(ContextId).toString,
      row.getString(ContextName),
      row.getString(Classpath),
      row.getString(State),
      new DateTime(row.getTimestamp(StartTime)),
      Option(row.getTimestamp(EndTime)).map(new DateTime(_)),
      errorData,
      cpBinaryInfos ++ legacyBinInfoData
    )
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = {
    val query = QB.select(
      JobId, ContextId, ContextName, AppName, BType, UploadTime, Classpath, State, StartTime, EndTime,
      Error, ErrorClass, ErrorStackTrace, Cp
    ).from(JobsTable).
      where(QB.eq(JobId, UUID.fromString(jobId))).
      limit(1)

    session.executeAsync(query).map { rs =>
      val row = rs.one()
      Option(row).map(rowToJobInfo)
    }
  }

  override def saveJobInfo(jobInfo: JobInfo): Unit = {
    val JobInfo(jobId, contextId, contextName, classPath, state,
        startTime, endTime, error, cp) = jobInfo
    val localDate: LocalDate = LocalDate.fromMillisSinceEpoch(jobInfo.startTime.getMillis)
    val binType = session.getCluster.getMetadata.getKeyspace(keyspace).getUserType(BinInfo)

    // convert to JavaList to avoid "Value 6 of type class scala.collection.immutable.$colon$colon
    // does not correspond to any CQL3 type" exception during insert
    val binInfos = JavaConversions.seqAsJavaList(
      jobInfo.cp.map(bin => {
      binType.newValue().
        setString(AppName, bin.appName).
        setString(BType, bin.binaryType.name).
        setTime(UploadTime, bin.uploadTime.getMillis)
    }))

    def fillInsert(insert: Insert): Insert = {
      insert.
        value(JobId, UUID.fromString(jobId)).
        value(ContextId, UUID.fromString(contextId)).
        value(ContextName, contextName).
        value(Classpath, classPath).
        value(State, state).
        value(StartTime, startTime.getMillis).
        value(StartDate, localDate).
        value(Cp, binInfos)

      endTime.foreach{e => insert.value(EndTime, e.getMillis)}
      error.foreach { err =>
        insert.value(Error, err.message)
        insert.value(ErrorClass, err.errorClass)
        insert.value(ErrorStackTrace, err.stackTrace)
      }
      insert
    }

    session.execute(fillInsert(insertInto(JobsTable)))
    session.execute(fillInsert(insertInto(JobsChronologicalTable)))
    session.execute(fillInsert(insertInto(JobsByContextIdTable)))
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = {
    val query = QB.select(Metadata.JobConfig).from(Metadata.JobsTable)
      .where(QB.eq(Metadata.JobId, UUID.fromString(jobId)))
    session.executeAsync(query).map {
      rs => Option(rs.one()).map(
        row => ConfigFactory.parseString(row.getString(Metadata.JobConfig)))
    }
  }

  override def getBinaryInfo(appName: String): Option[BinaryInfo] = {
    // Copied from the base JobDAO, feel free to optimize this (having in mind this specific storage type)
    Await.result(getApps, 60 seconds).get(appName).map(t => BinaryInfo(appName, t._1, t._2))
  }

  private def setup(config: Config): Session = {
    val cassandraConfig = config.getConfig("spark.jobserver.cassandra")
    val hosts = JListWrapper(cassandraConfig.getStringList("hosts"))
    val username = cassandraConfig.getString("user")
    val password = cassandraConfig.getString("password")
    val consistencyLevel = Try(
      ConsistencyLevel.valueOf(cassandraConfig.getString("consistency"))
    ).getOrElse(ConsistencyLevel.ONE)
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

    val contextsTableStatement = SchemaBuilder.createTable(ContextsTable).ifNotExists.
      addPartitionKey(ContextId, DataType.uuid).
      addClusteringColumn(StartTime, DataType.timestamp).
      addColumn(ContextName, DataType.text).
      addColumn(ContextConfig, DataType.text).
      addColumn(ActorAddress, DataType.text).
      addColumn(EndTime, DataType.timestamp).
      addColumn(State, DataType.text).
      addColumn(Error, DataType.text).
      withOptions().clusteringOrder(StartTime, Direction.DESC)

    session.execute(contextsTableStatement)

    val orderedContextsByNameTableStatement =
      SchemaBuilder.createTable(OrderedContextsByNameTable).ifNotExists.
      addPartitionKey(ContextName, DataType.text).
      addClusteringColumn(StartTime, DataType.timestamp).
      addClusteringColumn(ContextId, DataType.uuid).
      addColumn(ContextConfig, DataType.text).
      addColumn(ActorAddress, DataType.text).
      addColumn(EndTime, DataType.timestamp).
      addColumn(State, DataType.text).
      addColumn(Error, DataType.text).
      withOptions().clusteringOrder(StartTime, Direction.DESC)

    session.execute(orderedContextsByNameTableStatement)

    val orderedContextsByStateTableStateStatement =
      SchemaBuilder.createTable(OrderedContextsByStateTable).ifNotExists.
      addPartitionKey(State, DataType.text).
      addClusteringColumn(StartTime, DataType.timestamp).
      addClusteringColumn(ContextId, DataType.uuid).
      addColumn(ContextName, DataType.text).
      addColumn(ContextConfig, DataType.text).
      addColumn(ActorAddress, DataType.text).
      addColumn(EndTime, DataType.timestamp).
      addColumn(Error, DataType.text).
      withOptions().clusteringOrder(StartTime, Direction.DESC)

    session.execute(orderedContextsByStateTableStateStatement)

    val binariesDataTypeStatement: CreateType = SchemaBuilder.createType(BinInfo).ifNotExists.
      addColumn(AppName, DataType.text).
      addColumn(BType, DataType.text).
      addColumn(UploadTime, DataType.time)

    session.execute(binariesDataTypeStatement)

    val binInfoType = session.getCluster.getMetadata.getKeyspace(keyspace).getUserType(BinInfo)
    val jobsTableStatement = SchemaBuilder.createTable(JobsTable).ifNotExists.
      addPartitionKey(JobId, DataType.uuid).
      addColumn(ContextId, DataType.uuid).
      addColumn(ContextName, DataType.text).
      addColumn(AppName, DataType.text).
      addColumn(BType, DataType.text).
      addColumn(UploadTime, DataType.timestamp).
      addColumn(JobConfig, DataType.text).
      addColumn(Classpath, DataType.text).
      addColumn(State, DataType.text).
      addColumn(StartTime, DataType.timestamp).
      addColumn(StartDate, DataType.date).
      addColumn(EndTime, DataType.timestamp).
      addColumn(Error, DataType.text).
      addColumn(ErrorClass, DataType.text).
      addColumn(ErrorStackTrace, DataType.text).
      addColumn(Cp, DataType.frozenSet(binInfoType))

    session.execute(jobsTableStatement)

    val jobsChronologicalView = SchemaBuilder.createTable(JobsChronologicalTable).ifNotExists().
      addPartitionKey(StartDate, DataType.date).
      addClusteringColumn(StartTime, DataType.timestamp()).
      addClusteringColumn(JobId, DataType.uuid()).
      addColumn(ContextId, DataType.uuid).
      addColumn(ContextName, DataType.text).
      addColumn(AppName, DataType.text).
      addColumn(BType, DataType.text).
      addColumn(UploadTime, DataType.timestamp).
      addColumn(JobConfig, DataType.text).
      addColumn(Classpath, DataType.text).
      addColumn(State, DataType.text).
      addColumn(EndTime, DataType.timestamp).
      addColumn(Error, DataType.text).
      addColumn(ErrorClass, DataType.text).
      addColumn(ErrorStackTrace, DataType.text).
      addColumn(Cp, DataType.frozenSet(binInfoType)).
    withOptions().clusteringOrder(StartTime, Direction.DESC)

    session.execute(jobsChronologicalView)

    val jobsByContextIdView = SchemaBuilder.createTable(JobsByContextIdTable).ifNotExists().
      addPartitionKey(ContextId, DataType.uuid).
      addClusteringColumn(State, DataType.text).
      addClusteringColumn(JobId, DataType.uuid).
      addColumn(ContextName, DataType.text).
      addColumn(AppName, DataType.text).
      addColumn(BType, DataType.text).
      addColumn(UploadTime, DataType.timestamp).
      addColumn(JobConfig, DataType.text).
      addColumn(Classpath, DataType.text).
      addColumn(StartTime, DataType.timestamp).
      addColumn(StartDate, DataType.date).
      addColumn(EndTime, DataType.timestamp).
      addColumn(Error, DataType.text).
      addColumn(ErrorClass, DataType.text).
      addColumn(ErrorStackTrace, DataType.text).
      addColumn(Cp, DataType.frozenSet(binInfoType))

    session.execute(jobsByContextIdView)

    migrateJobInfo()
  }

  private def migrateJobInfo() = {
    val migrated = session.getCluster.getMetadata.getKeyspace(
      keyspace).getTable(JobsTable).getColumn(Cp)

    if (migrated == null) {
      val bt = session.getCluster.getMetadata.getKeyspace(keyspace).getUserType(BinInfo)
      val jobsTableStatement = SchemaBuilder.alterTable(JobsTable).addColumn(Cp).`type`(bt)
      session.execute(jobsTableStatement)

      val jobsChronologicalView = SchemaBuilder.alterTable(JobsChronologicalTable).addColumn(Cp).`type`(bt)
      session.execute(jobsChronologicalView)

      val jobsByContextIdView = SchemaBuilder.alterTable(JobsByContextIdTable).addColumn(Cp).`type`(bt)
      session.execute(jobsByContextIdView)
    }
  }
}
