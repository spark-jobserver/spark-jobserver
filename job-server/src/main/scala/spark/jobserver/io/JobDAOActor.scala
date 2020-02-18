package spark.jobserver.io

import java.io.File
import java.net.URI

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.common.akka.metrics.YammerMetrics
import spark.jobserver.io.CombinedDAO.rootDirConfPath
import spark.jobserver.util._
import spark.jobserver.util.DAOMetrics._
import spark.jobserver.JobManagerActor.ContextTerminatedException

import scala.concurrent.{Await, Future}

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
  case class GetJobInfo(jobId: String) extends JobDAORequest
  case class GetJobInfos(limit: Int, statuses: Option[String] = None) extends JobDAORequest
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
  case class GetBinaryInfosForCp(cp: Seq[String]) extends JobDAORequest

  //Responses
  sealed trait JobDAOResponse
  case class Apps(apps: Map[String, (BinaryType, DateTime)]) extends JobDAOResponse
  case class BinaryPath(binPath: String) extends JobDAOResponse
  case class JobInfos(jobInfos: Seq[JobInfo]) extends JobDAOResponse
  case class JobConfig(jobConfig: Option[Config]) extends JobDAOResponse
  case class LastBinaryInfo(lastBinaryInfo: Option[BinaryInfo]) extends JobDAOResponse
  case class ContextResponse(contextInfo: Option[ContextInfo]) extends JobDAOResponse
  case class ContextInfos(contextInfos: Seq[ContextInfo]) extends JobDAOResponse
  case class BinaryInfosForCp(binInfos: Seq[BinaryInfo]) extends JobDAOResponse
  case class BinaryNotFound(name: String) extends JobDAOResponse
  case class GetBinaryInfosForCpFailed(error: Throwable)

  case object InvalidJar extends JobDAOResponse
  case object JarStored extends JobDAOResponse
  case object JobConfigStored
  case object JobConfigStoreFailed

  sealed trait SaveResponse
  case object SavedSuccessfully extends SaveResponse
  case class SaveFailed(error: Throwable) extends SaveResponse

  def props(metadatadao: MetaDataDAO, binarydao: BinaryDAO, config: Config): Props = {
    Props(classOf[JobDAOActor], metadatadao, binarydao, config)
  }
}

class JobDAOActor(metaDataDAO: MetaDataDAO, binaryDAO: BinaryDAO, config: Config) extends InstrumentedActor
  with YammerMetrics with FileCacher {
  import JobDAOActor._
  import akka.pattern.pipe
  import context.dispatcher


  // Required by FileCacher
  val rootDirPath: String = config.getString(rootDirConfPath)
  val rootDirFile: File = new File(rootDirPath)


  implicit val daoTimeout = JobserverTimeouts.DAO_DEFAULT_TIMEOUT
  private val defaultAwaitTime = JobserverTimeouts.DAO_DEFAULT_TIMEOUT
  private val jobDAOHelper = new JobDAOActorHelper(metaDataDAO, binaryDAO, config)

  def wrappedReceive: Receive = {
    case SaveBinary(name, binaryType, uploadTime, binaryBytes) =>
      val recipient = sender()
      Future {
        jobDAOHelper.saveBinary(name, binaryType, uploadTime, binaryBytes)
      }.onComplete(recipient ! SaveBinaryResult(_))

    case DeleteBinary(name) =>
      val recipient = sender()
      val deleteResult = DeleteBinaryResult(Try {
        jobDAOHelper.deleteBinary(name)
      })
      recipient ! deleteResult

    case GetApps(typeFilter) =>
      val recipient = sender()
      Utils.timedFuture(binList){
        metaDataDAO.getBinaries.map(
          binaryInfos => binaryInfos.map(info => info.appName -> (info.binaryType, info.uploadTime)).toMap
        )
      }.map(apps => Apps(typeFilter.map(t => apps.filter(_._2._1 == t)).getOrElse(apps))).pipeTo(recipient)

    case GetBinaryPath(name, binaryType, uploadTime) =>
      val recipient = sender()
      val binPath = BinaryPath(jobDAOHelper.getBinaryPath(name, binaryType, uploadTime))
      recipient ! binPath

    case GetJobsByBinaryName(binName, statuses) =>
      Utils.timedFuture(jobQuery){
        metaDataDAO.getJobsByBinaryName(binName, statuses)
      }.map(JobInfos).pipeTo(sender)

    case SaveContextInfo(contextInfo) =>
      saveContextAndRespond(sender, contextInfo)

    case GetContextInfo(id) =>
      Utils.timedFuture(contextRead){
        metaDataDAO.getContext(id)
      }.map(ContextResponse).pipeTo(sender)

    case GetContextInfoByName(name) =>
      Utils.timedFuture(contextQuery){
        metaDataDAO.getContextByName(name)
      }.map(ContextResponse).pipeTo(sender)

    case GetContextInfos(limit, statuses) =>
      Utils.timedFuture(contextList){
        metaDataDAO.getContexts(limit, statuses)
      }.map(ContextInfos).pipeTo(sender)

    case SaveJobInfo(jobInfo) =>
      Utils.timedFuture(jobWrite) {
        metaDataDAO.saveJob(jobInfo)
      }.pipeTo(sender)

    case GetJobInfo(jobId) =>
      Utils.timedFuture(jobRead){
        metaDataDAO.getJob(jobId)
      }.pipeTo(sender)

    case GetJobInfos(limit, statuses) =>
      Utils.timedFuture(jobList){
        metaDataDAO.getJobs(limit, statuses)
      }.map(JobInfos).pipeTo(sender)

    case SaveJobConfig(jobId, jobConfig) =>
      val recipient = sender()
      Utils.usingTimer(configWrite){ () =>
        metaDataDAO.saveJobConfig(jobId, jobConfig)
      }.onComplete {
        case Failure(_) | Success(false) => recipient ! JobConfigStoreFailed
        case Success(true) => recipient ! JobConfigStored
      }

    case GetJobConfig(jobId) =>
      Utils.timedFuture(configRead){
        metaDataDAO.getJobConfig(jobId)
      }.map(JobConfig).pipeTo(sender)

    case GetLastBinaryInfo(name) =>
      Utils.usingTimer(binRead){ () =>
        metaDataDAO.getBinary(name)
      }.map(LastBinaryInfo(_)).pipeTo(sender)

    case CleanContextJobInfos(contextId, endTime) =>
      Utils.timedFuture(jobQuery){
        metaDataDAO.getJobsByContextId(contextId, Some(JobStatus.getNonFinalStates()))
      }.map { infos: Seq[JobInfo] =>
        logger.info("cleaning {} non-final state job(s) {} for context {}",
          infos.size.toString, infos.map(_.jobId).mkString(", "), contextId)
        for (info <- infos) {
          val updatedInfo = info.copy(
            state = JobStatus.Error,
            endTime = Some(endTime),
            error = Some(ErrorData(ContextTerminatedException(contextId))))
          self ! SaveJobInfo(updatedInfo)
        }
      }

    case GetJobInfosByContextId(contextId, jobStatuses) =>
      Utils.timedFuture(jobQuery){
        metaDataDAO.getJobsByContextId(contextId, jobStatuses)
      }.map(JobInfos).pipeTo(sender)

    case UpdateContextById(contextId: String, attributes: ContextModifiableAttributes) =>
      val recipient = sender()
      Utils.timedFuture(contextRead){
        metaDataDAO.getContext(contextId)
      }.map(ContextResponse).onComplete {
        case Success(ContextResponse(Some(contextInfo))) =>
          saveContextAndRespond(recipient, copyAttributes(contextInfo, attributes))
        case Success(ContextResponse(None)) =>
          logger.warn(s"Context with id $contextId doesn't exist")
          recipient ! SaveFailed(NoMatchingDAOObjectException())
        case Failure(t) =>
          logger.error(s"Failed to get context $contextId by Id", t)
          recipient ! SaveFailed(t)
      }

    case GetBinaryInfosForCp(cp) =>
      val recipient = sender()
      val currentTime = DateTime.now()
      Try {
        cp.flatMap(name => {
          val uri = new URI(name)
          uri.getScheme match {
            // protocol like "local" is supported in Spark for Jar loading, but not supported in Java.
            case "local" =>
              Some(BinaryInfo("file://" + uri.getPath, BinaryType.URI, currentTime, None))
            case null =>
              val binInfo = Await.result(
                Utils.usingTimer(binRead){ () => metaDataDAO.getBinary(name)}, defaultAwaitTime)
              if (binInfo.isEmpty) {
                throw NoSuchBinaryException(name)
              }
              binInfo
            case _ => Some(BinaryInfo(name, BinaryType.URI, currentTime, None))
          }
        })
      } match {
        case Success(binInfos) => recipient ! BinaryInfosForCp(binInfos)
        case Failure(NoSuchBinaryException(name)) => recipient ! BinaryNotFound(name)
        case Failure(exception) =>
          logger.error(exception.getMessage)
          recipient ! GetBinaryInfosForCpFailed(exception)
    }

    case anotherEvent => logger.info(s"Ignoring unknown event type: $anotherEvent")
  }

  private def saveContextAndRespond(recipient: ActorRef, contextInfo: ContextInfo) = {
    Utils.usingTimer(contextWrite) {
      () => metaDataDAO.saveContext(contextInfo)
    }.onComplete {
        case Success(true) => recipient ! SavedSuccessfully
        case Success(false) =>
          logger.error(s"Failed to save context (${contextInfo.id}). DAO returned false.")
          recipient ! SaveFailed(SaveContextException(contextInfo.id))
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
