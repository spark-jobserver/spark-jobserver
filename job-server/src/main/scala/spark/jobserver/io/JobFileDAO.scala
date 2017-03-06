package spark.jobserver.io

import com.typesafe.config._
import java.io._
import java.nio.file.{Files, Paths}

import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

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

  private val rootDir = config.getString("spark.jobserver.filedao.rootdir")
  private val rootDirFile = new File(rootDir)
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
        case e: Exception      => throw e

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
    if(binaryType == BinaryType.Jar) {
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

  override def retrieveBinaryFile(appName: String, binaryType: BinaryType, uploadTime: DateTime): String =
    new File(rootDir, createJarName(appName, uploadTime) + s".${binaryType.extension}").getAbsolutePath

  private def createJarName(appName: String, uploadTime: DateTime): String =
    appName + "-" + uploadTime.toString().replace(':', '_')

  override def saveJobInfo(jobInfo: JobInfo) {
    writeJobInfo(jobsOutputStream, jobInfo)
    jobs(jobInfo.jobId) = jobInfo
  }

  private def writeJobInfo(out: DataOutputStream, jobInfo: JobInfo) {
    out.writeUTF(jobInfo.jobId)
    out.writeUTF(jobInfo.contextName)
    writeJarInfo(out, jobInfo.binaryInfo)
    out.writeUTF(jobInfo.classPath)
    out.writeLong(jobInfo.startTime.getMillis)
    val time = if (jobInfo.endTime.isEmpty) jobInfo.startTime.getMillis else jobInfo.endTime.get.getMillis
    out.writeLong(time)
    val errorStr = if (jobInfo.error.isEmpty) "" else jobInfo.error.get.toString
    out.writeUTF(errorStr)
  }

  private def readError(in: DataInputStream) = {
    val error = in.readUTF()
    if (error == "") None else Some(new Throwable(error))
  }

  private def readJobInfo(in: DataInputStream) = JobInfo(
    in.readUTF,
    in.readUTF,
    readJarInfo(in),
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
      case Some(JobStatus.Running) => {
        allJobs.filter(jobInfo => !jobInfo.endTime.isDefined && !jobInfo.error.isDefined)
      }
      case Some(JobStatus.Error) => allJobs.filter(_.error.isDefined)
      case Some(JobStatus.Finished) => {
        allJobs.filter(jobInfo => jobInfo.endTime.isDefined && !jobInfo.error.isDefined)
      }
      case _ => allJobs
    }
    filterJobs.take(limit)
  }

  override def saveJobConfig(jobId: String, jobConfig: Config) {
    writeJobConfig(jobConfigsOutputStream, jobId, jobConfig)
    configs(jobId) = jobConfig
  }

  override def getJobConfigs: Future[Map[String, Config]] = Future { configs.toMap }

  private def writeJobConfig(out: DataOutputStream, jobId: String, jobConfig: Config) {
    out.writeUTF(jobId)
    out.writeUTF(jobConfig.root().render(ConfigRenderOptions.concise()))
  }

  private def readJobConfig(in: DataInputStream): (String, Config) = (
    in.readUTF,
    ConfigFactory.parseString(in.readUTF)
  )

  override def getBinaryContent(appName: String, binaryType: BinaryType,
                                uploadTime: DateTime): Array[Byte] = {
    Files.readAllBytes(Paths.get(retrieveBinaryFile(appName, binaryType, uploadTime)))
  }

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
