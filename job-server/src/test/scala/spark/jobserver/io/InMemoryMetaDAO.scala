package spark.jobserver.io

import com.typesafe.config.Config
import org.joda.time.DateTime

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class InMemoryMetaDAO extends MetaDataDAO {
  val contextInfos = mutable.HashMap.empty[String, ContextInfo]
  val jobInfos = mutable.HashMap.empty[String, JobInfo]
  val jobConfigs = mutable.HashMap.empty[String, Config]
  var binaries = mutable.HashMap.empty[(String, DateTime), BinaryInfo]

  override def saveContext(contextInfo: ContextInfo): Future[Boolean] = Future {
      contextInfos(contextInfo.id) = contextInfo
      true
  }

  override def getContext(id: String): Future[Option[ContextInfo]] = Future {
      contextInfos.get(id)
  }

  override def getContextByName(name: String): Future[Option[ContextInfo]] = Future {
    contextInfos.values.toSeq.sortWith(sortTime).filter(_.name == name).headOption
  }

  override def getContexts(limit: Option[Int], statuses: Option[Seq[String]]): Future[Seq[ContextInfo]] =
    Future {
      val allContexts = contextInfos.values.toSeq.sortWith(sortTime)
      val filteredContexts = statuses match {
        case Some(statuses) =>
          allContexts.filter(j => statuses.contains(j.state))
        case _ => allContexts
      }

      limit match {
        case Some(l) => filteredContexts.take(l)
        case _ => filteredContexts
      }
  }

  override def saveJob(jobInfo: JobInfo): Future[Boolean] = Future {
    jobInfos(jobInfo.jobId) = jobInfo
    true
  }

  override def getJob(id: String): Future[Option[JobInfo]] = Future {
    jobInfos.get(id)
  }

  override def getJobs(limit: Int, status: Option[String]): Future[Seq[JobInfo]] = Future {
    val allJobs = jobInfos.values.toSeq.sortBy(_.startTime.toString())
    val filterJobs = status match {
      case Some(JobStatus.Running) => {
        allJobs.filter(jobInfo => jobInfo.endTime.isEmpty && jobInfo.error.isEmpty)
      }
      case Some(JobStatus.Error) => allJobs.filter(_.error.isDefined)
      case Some(JobStatus.Finished) => {
        allJobs.filter(jobInfo => jobInfo.endTime.isDefined && jobInfo.error.isEmpty)
      }
      case _ => allJobs
    }
    filterJobs.take(limit)
  }

  override def getJobsByContextId(contextId: String, statuses: Option[Seq[String]]): Future[Seq[JobInfo]] =
    Future {
      jobInfos.values.toSeq.filter(j => {
        (contextId, statuses) match {
          case (contextId, Some(statuses)) => contextId == j.contextId && statuses.contains(j.state)
          case _ => contextId == j.contextId
        }
      })
  }

  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]]): Future[Seq[JobInfo]] = {
    throw new NotImplementedError()
  }

  override def saveJobConfig(id: String, config: Config): Future[Boolean] = Future {
    jobConfigs(id) = config
    true
  }

  override def getJobConfig(id: String): Future[Option[Config]] = Future {
    jobConfigs.get(id)
  }

  override def getBinary(name: String): Future[Option[BinaryInfo]] = {
    for {
      binaries <- getBinaries
    } yield binaries.filter(_.appName == name).headOption
  }

  override def getBinaries: Future[Seq[BinaryInfo]] = Future {
    binaries.values
      .groupBy(_.appName)
      .map {
        case (appName, binaryInfosWithSameName) =>
          appName -> binaryInfosWithSameName.toSeq.sortBy(_.uploadTime.getMillis)(Ordering[Long].reverse).head
      }.values.toList
  }

  override def getBinariesByStorageId(storageId: String): Future[Seq[BinaryInfo]] = Future {
    binaries.values.filter {
      binaryInfo =>
        binaryInfo.binaryStorageId match {
          case None => false
          case Some(id) => id == storageId
        }
    }.toSeq
  }

  override def saveBinary(name: String, binaryType: BinaryType, uploadTime: DateTime,
      binaryStorageId: String): Future[Boolean] = Future {
    binaries((name, uploadTime)) = BinaryInfo(name, binaryType, uploadTime, Some(binaryStorageId))
    true
  }

  override def deleteBinary(name: String): Future[Boolean] = Future {
    binaries = binaries.filter(_._1._1 != name)
    true
  }

  private def sortTime(c1: ContextInfo, c2: ContextInfo): Boolean = {
    // If both dates are the same then it will return false
    c1.startTime.isAfter(c2.startTime)
  }
}
