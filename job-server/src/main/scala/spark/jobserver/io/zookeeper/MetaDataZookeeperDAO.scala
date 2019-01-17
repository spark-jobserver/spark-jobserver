package spark.jobserver.io.zookeeper

import com.typesafe.config.Config
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.io._
import spark.jobserver.util.JsonProtocols

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MetaDataZookeeperDAO {
  private val baseDirPath = "spark.jobserver.zookeeper.dir"
  private val connectionStringPath = "spark.jobserver.zookeeper.connection-string"
  private val binariesDir = "/binaries"
  private val contextsDir = "/contexts"
  private val jobsDir = "/jobs"
}

class MetaDataZookeeperDAO(config: Config) extends MetaDataDAO {
  import MetaDataZookeeperDAO._
  import JsonProtocols._

  private val logger = LoggerFactory.getLogger(getClass)

  if (!config.hasPath(baseDirPath)) {
    throw new InvalidConfiguration(
      s"To use ZooKeeperDAO please specify ZK root dir for DAO in configuration file: $baseDirPath"
    )
  }

  if (!config.hasPath(connectionStringPath)) {
    throw new InvalidConfiguration(
      s"To use ZooKeeperDAO please specify ZooKeeper connection string: $connectionStringPath"
    )
  }

  private val connectionString = config.getString(connectionStringPath)
  private val zookeeperUtils = new ZookeeperUtils(connectionString, config.getString(baseDirPath))

  override def saveContext(contextInfo: ContextInfo): Future[Boolean] = {
    throw new NotImplementedError()
  }

  override def getContext(id: String): Future[Option[ContextInfo]] = {
    throw new NotImplementedError()
  }

  override def getContextByName(name: String): Future[Option[ContextInfo]] = {
    throw new NotImplementedError()
  }

  override def getContexts(limit: Option[Int], statuses: Option[Seq[String]]): Future[Seq[ContextInfo]] = {
    Future{Seq.empty[ContextInfo]}
  }

  override def saveJob(jobInfo: JobInfo): Future[Boolean] = {
    throw new NotImplementedError()
  }

  override def getJob(id: String): Future[Option[JobInfo]] = {
    throw new NotImplementedError()
  }

  override def getJobs(limit: Int, status: Option[String]): Future[Seq[JobInfo]] = {
    throw new NotImplementedError()
  }

  override def getJobsByContextId(contextId: String, statuses: Option[Seq[String]]): Future[Seq[JobInfo]] = {
    throw new NotImplementedError()
  }

  override def saveJobConfig(id: String, config: Config): Future[Boolean] = {
    throw new NotImplementedError()
  }

  override def getJobConfig(id: String): Future[Option[Config]] = {
    throw new NotImplementedError()
  }

  override def getBinary(name: String): Future[Option[BinaryInfo]] = {
    Future {
      zookeeperUtils.read[Seq[BinaryInfo]](s"$binariesDir/$name") match {
        case Some(infoForBinary) =>
          Some(infoForBinary.sortWith(_.uploadTime.getMillis > _.uploadTime.getMillis).head)
        case None =>
          logger.info(s"Didn't find any information for the path: $binariesDir/$name")
          None
      }
    }
  }

  override def getBinaries: Future[Seq[BinaryInfo]] = {
    Future {
      zookeeperUtils.list(binariesDir).map(
        name => zookeeperUtils.read[Seq[BinaryInfo]](s"$binariesDir/$name")
      ).filter(_.isDefined).map(
        binary => binary.get.head
      )
    }
  }

  override def getBinariesByStorageId(storageId: String): Future[Seq[BinaryInfo]] = {
    Future {
      zookeeperUtils.list(s"$binariesDir").flatMap(
        name => zookeeperUtils.read[Seq[BinaryInfo]](s"$binariesDir/$name").getOrElse(Seq.empty[BinaryInfo])
      ).filter(_.binaryStorageId.getOrElse("") == storageId).
        groupBy(_.appName).map(_._2.head).toSeq
    }
  }

  override def saveBinary(name: String, binaryType: BinaryType,
                          uploadTime: DateTime, binaryStorageId: String): Future[Boolean] = {
    Future {
      val oldInfoForBinary = zookeeperUtils.read[Seq[BinaryInfo]](s"$binariesDir/$name")
      val newBinary = BinaryInfo(name, binaryType, uploadTime, Some(binaryStorageId))
      zookeeperUtils.write[Seq[BinaryInfo]](
        newBinary +: oldInfoForBinary.getOrElse(Seq.empty[BinaryInfo]),
        s"$binariesDir/$name")
    }
  }

  override def deleteBinary(name: String): Future[Boolean] = {
    Future {
      zookeeperUtils.read[Seq[BinaryInfo]](s"$binariesDir/$name") match {
          case Some(_) => zookeeperUtils.delete(s"$binariesDir/$name")
          case None =>
            logger.info(s"Didn't find any information for the path: $binariesDir/$name")
            false
        }
    }
  }
}
