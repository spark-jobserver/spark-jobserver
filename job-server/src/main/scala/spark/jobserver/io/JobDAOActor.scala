package spark.jobserver.io

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import org.joda.time.DateTime
import spark.jobserver.ZookeeperMigrationActor

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.util.NoMatchingDAOObjectException

import scala.concurrent.Future

object JobDAOActor {

  //Requests
  sealed trait JobDAORequest
  case class SaveBinary(appName: String,
                        binaryType: BinaryType,
                        uploadTime: DateTime,
                        jarBytes: Array[Byte]) extends JobDAORequest
  case class SaveBinaryResult(outcome: Try[Unit])

  case class DeleteBinary(appName: String) extends JobDAORequest
  case class DeleteBinaryResult(outcome: Try[Unit])

  case class GetApps(typeFilter: Option[BinaryType]) extends JobDAORequest
  case class GetBinaryPath(appName: String,
                           binaryType: BinaryType,
                           uploadTime: DateTime) extends JobDAORequest

  case class SaveJobInfo(jobInfo: JobInfo) extends JobDAORequest
  case class GetJobInfos(limit: Int) extends JobDAORequest
  case class GetJobInfosByContextId(contextId: String, jobStatuses: Option[Seq[String]]) extends JobDAORequest

  case class SaveJobConfig(jobId: String, jobConfig: Config) extends JobDAORequest
  case class GetJobConfig(jobId: String) extends JobDAORequest
  case class CleanContextJobInfos(contextId: String, endTime: DateTime)

  case class GetLastBinaryInfo(appName: String) extends JobDAORequest
  case class SaveContextInfo(contextInfo: ContextInfo)  extends JobDAORequest
  case class UpdateContextById(contextId: String, attributes: ContextModifiableAttributes)
  case class GetContextInfo(id: String) extends JobDAORequest
  case class GetContextInfoByName(name: String) extends JobDAORequest
  case class GetContextInfos(limit: Option[Int] = None,
      statuses: Option[Seq[String]] = None) extends JobDAORequest
  case class GetJobsByBinaryName(appName: String, statuses: Option[Seq[String]] = None) extends JobDAORequest

  //Responses
  sealed trait JobDAOResponse
  case class Apps(apps: Map[String, (BinaryType, DateTime)]) extends JobDAOResponse
  case class BinaryPath(binPath: String) extends JobDAOResponse
  case class JobInfos(jobInfos: Seq[JobInfo]) extends JobDAOResponse
  case class JobConfig(jobConfig: Option[Config]) extends JobDAOResponse
  case class LastBinaryInfo(lastBinaryInfo: Option[BinaryInfo]) extends JobDAOResponse
  case class ContextResponse(contextInfo: Option[ContextInfo]) extends JobDAOResponse
  case class ContextInfos(contextInfos: Seq[ContextInfo]) extends JobDAOResponse

  case object InvalidJar extends JobDAOResponse
  case object JarStored extends JobDAOResponse

  sealed trait SaveResponse
  case object SavedSuccessfully extends SaveResponse
  case class SaveFailed(error: Throwable) extends SaveResponse

  def props(dao: JobDAO, migrationActor: ActorRef, migrationAddress: String = ""): Props =
    Props(classOf[JobDAOActor], dao, migrationActor, migrationAddress)
}

class JobDAOActor(dao: JobDAO, migrationActor: ActorRef, migrationAddress: String) extends InstrumentedActor {
  import JobDAOActor._
  import akka.pattern.pipe
  import context.dispatcher

  implicit val daoTimeout = 60 seconds

  def wrappedReceive: Receive = {
    case SaveBinary(appName, binaryType, uploadTime, jarBytes) =>
      val recipient = sender()
      Future {
        dao.saveBinary(appName, binaryType, uploadTime, jarBytes)
      }.onComplete(recipient ! SaveBinaryResult(_))

    case DeleteBinary(appName) =>
      sender ! DeleteBinaryResult(Try(dao.deleteBinary(appName)))

    case GetApps(typeFilter) =>
      dao.getApps.map(apps => Apps(typeFilter.map(t => apps.filter(_._2._1 == t)).getOrElse(apps))).
        pipeTo(sender)

    case GetBinaryPath(appName, binType, uploadTime) =>
      sender() ! BinaryPath(dao.getBinaryFilePath(appName, binType, uploadTime))

    case GetJobsByBinaryName(binName, statuses) =>
      dao.getJobsByBinaryName(binName, statuses).map(JobInfos).pipeTo(sender)

    case SaveContextInfo(contextInfo) =>
      saveContextAndRespond(sender, contextInfo)
      if (migrationAddress == "") {
        migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(contextInfo)
      } else {
        logger.debug(s"[Zookeeper] Resolving migration address")
        context.actorSelection(migrationAddress) ! ZookeeperMigrationActor.SaveContextInfoInZK(contextInfo)
      }

    case GetContextInfo(id) =>
      dao.getContextInfo(id).map(ContextResponse).pipeTo(sender)

    case GetContextInfoByName(name) =>
      dao.getContextInfoByName(name).map(ContextResponse).pipeTo(sender)

    case GetContextInfos(limit, statuses) =>
      dao.getContextInfos(limit, statuses).map(ContextInfos).pipeTo(sender)

    case SaveJobInfo(jobInfo) =>
      sender ! dao.saveJobInfo(jobInfo)
      if (migrationAddress == "") {
        migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(jobInfo)
      } else {
        logger.debug(s"[Zookeeper] Resolving migration address")
        context.actorSelection(migrationAddress) ! ZookeeperMigrationActor.SaveJobInfoInZK(jobInfo)
      }

    case GetJobInfos(limit) =>
      dao.getJobInfos(limit).map(JobInfos).pipeTo(sender)

    case SaveJobConfig(jobId, jobConfig) =>
      dao.saveJobConfig(jobId, jobConfig)

    case GetJobConfig(jobId) =>
      dao.getJobConfig(jobId).map(JobConfig).pipeTo(sender)

    case GetLastBinaryInfo(appName) =>
      sender() ! LastBinaryInfo(dao.getBinaryInfo(appName))

    case CleanContextJobInfos(contextId, endTime) =>
      dao.cleanRunningJobInfosForContext(contextId, endTime)

    case GetJobInfosByContextId(contextId, jobStatuses) =>
      dao.getJobInfosByContextId(contextId, jobStatuses).map(JobInfos).pipeTo(sender)

    case UpdateContextById(contextId: String, attributes: ContextModifiableAttributes) =>
      val recipient = sender()
      dao.getContextInfo(contextId).map(ContextResponse).onComplete {
        case Success(ContextResponse(Some(contextInfo))) =>
          saveContextAndRespond(recipient, copyAttributes(contextInfo, attributes))
        case Success(ContextResponse(None)) =>
          logger.warn(s"Context with id $contextId doesn't exist")
          recipient ! SaveFailed(NoMatchingDAOObjectException())
        case Failure(t) =>
          logger.error(s"Failed to get context $contextId by Id", t)
          recipient ! SaveFailed(t)
      }
  }

  private def saveContextAndRespond(recipient: ActorRef, contextInfo: ContextInfo) = {
    Try(dao.saveContextInfo(contextInfo)) match {
      case Success(_) => recipient ! SavedSuccessfully
      case Failure(t) =>
        logger.error(s"Failed to save context (${contextInfo.id}) in DAO", t)
        recipient ! SaveFailed(t)
    }
  }

  private def copyAttributes(contextInfo: ContextInfo,
                 attributes: ContextModifiableAttributes): ContextInfo = {
    contextInfo.copy(
      actorAddress = getOrDefault(attributes.actorAddress, contextInfo.actorAddress),
      endTime = getOrDefault(attributes.endTime, contextInfo.endTime),
      state = getOrDefault(attributes.state, contextInfo.state),
      error = getOrDefault(attributes.error, contextInfo.error))
  }

  private def getOrDefault[T](value: T, default: T): T = {
    value match {
      case Some(_) => value
      case None => default
      case s: String if s.isEmpty => default
      case _: String => value
    }
  }
}
