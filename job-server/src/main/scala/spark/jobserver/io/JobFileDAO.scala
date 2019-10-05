package spark.jobserver.io

import java.io._

import com.typesafe.config._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * NB This class does NOT support persisting binary types
  * other than Jars (such as Python eggs).
  * @param config
  */
@deprecated("Use JobSqlDAO instead for stability.", "0.7.0")
class JobFileDAO(config: Config) extends JobDAO {
  private val logger = LoggerFactory.getLogger(getClass)

  // appName to its set of upload times. Decreasing times in the seq.
  private val apps = mutable.HashMap.empty[String, Seq[DateTime]]
  // jobId to its JobInfo
  private val jobs = mutable.HashMap.empty[String, JobInfo]
  // jobId to its Config
  private val configs = mutable.HashMap.empty[String, Config]

  val rootDir = config.getString("spark.jobserver.filedao.rootdir")
  val rootDirFile = new File(rootDir)
  logger.info("rootDir is " + rootDirFile.getAbsolutePath)

  private val jarsFile = new File(rootDirFile, "jars.data")
  private var jarsOutputStream: DataOutputStream = null
  private val jobsFile = new File(rootDirFile, "jobs.data")
  private var jobsOutputStream: DataOutputStream = null
  private val jobConfigsFile = new File(rootDirFile, "configs.data")
  private var jobConfigsOutputStream: DataOutputStream = null

  init()

  private def init() {
    // create the data directory if it doesn't exist
    if (!rootDirFile.exists()) {
      if (!rootDirFile.mkdirs()) {
        throw new RuntimeException("Could not create directory " + rootDir)
      }
    }

    // read back all apps info during startup
    if (jarsFile.exists()) {
      val in = new DataInputStream(new BufferedInputStream(new FileInputStream(jarsFile)))
      try {
        while (true) {
          val jarInfo = readJarInfo(in)
          addJar(jarInfo.appName, jarInfo.uploadTime)
        }
      } catch {
        case e: EOFException => // do nothing
      } finally {
        in.close()
      }
    }

    // read back all jobs info during startup
    if (jobsFile.exists()) {
      val in = new DataInputStream(new BufferedInputStream(new FileInputStream(jobsFile)))
      try {
        while (true) {
          val jobInfo = readJobInfo(in)
          jobs(jobInfo.jobId) = jobInfo
        }
      } catch {
        case eof: EOFException => // do nothing
        case e: Exception => throw e

      } finally {
        in.close()
      }
    }

    // read back all job configs during startup
    if (jobConfigsFile.exists()) {
      val in = new DataInputStream(new BufferedInputStream(new FileInputStream(jobConfigsFile)))
      try {
        while (true) {
          val (jobId, jobConfig) = readJobConfig(in)
          configs(jobId) = jobConfig
        }
      } catch {
        case eof: EOFException => // do nothing
      } finally {
        in.close()
      }
    }

    // Don't buffer the stream. I want the apps meta data log directly into the file.
    // Otherwise, server crash will lose the buffer data.
    jarsOutputStream = new DataOutputStream(new FileOutputStream(jarsFile, true))
    jobsOutputStream = new DataOutputStream(new FileOutputStream(jobsFile, true))
    jobConfigsOutputStream = new DataOutputStream(new FileOutputStream(jobConfigsFile, true))
  }

  override def saveBinary(appName: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          jarBytes: Array[Byte]) {
    if (binaryType == BinaryType.Jar) {
      // The order is important. Save the jar file first and then log it into jobsFile.
      val outFile = new File(rootDir, createJarName(appName, uploadTime) + s".${binaryType.extension}")
      val bos = new BufferedOutputStream(new FileOutputStream(outFile))
      try {
        logger.debug("Writing {} bytes to file {}", jarBytes.length, outFile.getPath)
        bos.write(jarBytes)
        bos.flush()
      } finally {
        bos.close()
      }

      // log it into jobsFile
      writeJarInfo(jarsOutputStream, BinaryInfo(appName, binaryType, uploadTime))

      // track the new jar in memory
      addJar(appName, uploadTime)
    } else {
      throw new Exception("JobFileDAO only supports Jars. To use other binary types please use JobSQLDao.")
    }
  }

  private def writeJarInfo(out: DataOutputStream, jarInfo: BinaryInfo) {
    out.writeUTF(jarInfo.appName)
    out.writeLong(jarInfo.uploadTime.getMillis)
  }

  private def readJarInfo(in: DataInputStream) =
    BinaryInfo(in.readUTF, BinaryType.Jar, new DateTime(in.readLong))

  private def addJar(appName: String, uploadTime: DateTime) {
    if (apps.contains(appName)) {
      apps(appName) = uploadTime +: apps(appName) // latest time comes first
    } else {
      apps(appName) = Seq(uploadTime)
    }
  }

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = Future {
    apps.map {
      case (appName, uploadTimes) =>
        appName -> (BinaryType.Jar, uploadTimes.head)
    }.toMap
  }

  override def getBinaryFilePath(appName: String, binaryType: BinaryType, uploadTime: DateTime): String =
    new File(rootDir, createJarName(appName, uploadTime) + s".${binaryType.extension}").getAbsolutePath

  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
      Future[Seq[JobInfo]] = {
    throw new NotImplementedError()
  }

  private def createJarName(appName: String, uploadTime: DateTime): String =
    appName + "-" + uploadTime.toString().replace(':', '_')

  override def saveContextInfo(contextInfo: ContextInfo): Unit = {
    throw new NotImplementedError
  }

  override def getContextInfo(id: String): Future[Option[ContextInfo]] = {
    throw new NotImplementedError;
  }

  override def getContextInfos(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
    Future[Seq[ContextInfo]] = {
    throw new NotImplementedError;
  }

  override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = {
    throw new NotImplementedError;
  }


  override def saveJobInfo(jobInfo: JobInfo) {
    writeJobInfo(jobsOutputStream, jobInfo)
    jobs(jobInfo.jobId) = jobInfo
  }

  private def writeJobInfo(out: DataOutputStream, jobInfo: JobInfo) {
    out.writeUTF(jobInfo.jobId)
    out.writeUTF(jobInfo.contextId)
    out.writeUTF(jobInfo.contextName)
    writeJarInfo(out, jobInfo.binaryInfo)
    out.writeUTF(jobInfo.classPath)
    out.writeUTF(jobInfo.state)
    out.writeLong(jobInfo.startTime.getMillis)
    val time = if (jobInfo.endTime.isEmpty) jobInfo.startTime.getMillis else jobInfo.endTime.get.getMillis
    out.writeLong(time)
    out.writeUTF(jobInfo.error.map(_.message).getOrElse(""))
    out.writeUTF(jobInfo.error.map(_.errorClass).getOrElse(""))
    out.writeUTF(jobInfo.error.map(_.stackTrace).getOrElse(""))
  }

  private def readError(in: DataInputStream) = {
    val error = Some(in.readUTF()).filter(_.isEmpty)
    val errorClass = in.readUTF()
    val errorStackTrace = in.readUTF()
    error.map(ErrorData(_, errorClass, errorStackTrace))
  }

  private def readJobInfo(in: DataInputStream) = JobInfo(
    in.readUTF,
    in.readUTF,
    in.readUTF,
    readJarInfo(in),
    in.readUTF,
    in.readUTF,
    new DateTime(in.readLong),
    Some(new DateTime(in.readLong)),
    readError(in))

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = Future {
    jobs.get(jobId)
  }

  override def getJobInfos(limit: Int, statusOpt: Option[String] = None): Future[Seq[JobInfo]] = Future {
    val allJobs = jobs.values.toSeq.sortBy(-_.startTime.getMillis)
    val filterJobs = statusOpt match {
      case Some(state) => allJobs.filter(_.state.equals(state))
      case _ => allJobs
    }
    filterJobs.take(limit)
  }

  override def getJobInfosByContextId(
      contextId: String, jobStatuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = Future {
    jobs.values.toSeq.filter(j => {
      (contextId, jobStatuses) match {
        case (contextId, Some(statuses)) => contextId == j.contextId && statuses.contains(j.state)
        case _ => contextId == j.contextId
      }
    })
  }

  override def saveJobConfig(jobId: String, jobConfig: Config) {
    writeJobConfig(jobConfigsOutputStream, jobId, jobConfig)
    configs(jobId) = jobConfig
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = Future {
    configs.get(jobId)
  }

  override def getBinaryInfo(appName: String): Option[BinaryInfo] = {
    apps(appName).headOption.map(uploadTime => BinaryInfo(appName, BinaryType.Jar, uploadTime))
  }

  private def writeJobConfig(out: DataOutputStream, jobId: String, jobConfig: Config) {
    out.writeUTF(jobId)
    out.writeUTF(jobConfig.root().render(ConfigRenderOptions.concise()))
  }

  private def readJobConfig(in: DataInputStream): (String, Config) = (
    in.readUTF,
    ConfigFactory.parseString(in.readUTF)
  )

  /**
    * Delete a jar.
    *
    * @param appName
    */
  override def deleteBinary(appName: String): Unit = {
    val dir = new File(rootDir)
    val binaries = dir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith(appName)
    })
    if (binaries != null) {
      binaries.foreach(f => f.delete())
    }
  }
}
