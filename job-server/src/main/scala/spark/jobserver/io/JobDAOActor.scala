package spark.jobserver.io

import akka.actor.Props
import com.typesafe.config.Config
import org.joda.time.DateTime
import spark.jobserver.JobManagerActor.JobKilledException

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

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

  case class SaveJobConfig(jobId: String, jobConfig: Config) extends JobDAORequest
  case class GetJobConfig(jobId: String) extends JobDAORequest
  case class CleanContextJobInfos(contextName: String, endTime: DateTime)

  case class GetLastUploadTimeAndType(appName: String) extends JobDAORequest
  case class SaveContextInfo(contextInfo: ContextInfo) extends JobDAORequest
  case class GetContextInfo(id: String) extends JobDAORequest
  case class GetContextInfoByName(name: String) extends JobDAORequest
  case class GetContextInfos(limit: Option[Int] = None,
      statusOpt: Option[String] = None) extends JobDAORequest

  //Responses
  sealed trait JobDAOResponse
  case class Apps(apps: Map[String, (BinaryType, DateTime)]) extends JobDAOResponse
  case class BinaryPath(binPath: String) extends JobDAOResponse
  case class JobInfos(jobInfos: Seq[JobInfo]) extends JobDAOResponse
  case class JobConfig(jobConfig: Option[Config]) extends JobDAOResponse
  case class LastUploadTimeAndType(uploadTimeAndType: Option[(DateTime, BinaryType)]) extends JobDAOResponse
  case class ContextResponse(contextInfo: Option[ContextInfo]) extends JobDAOResponse
  case class ContextInfos(contextInfos: Seq[ContextInfo]) extends JobDAOResponse

  case object InvalidJar extends JobDAOResponse
  case object JarStored extends JobDAOResponse

  def props(dao: JobDAO): Props = Props(classOf[JobDAOActor], dao)
}

class JobDAOActor(dao: JobDAO) extends InstrumentedActor {
  import JobDAOActor._
  import akka.pattern.pipe
  import context.dispatcher

  implicit val daoTimeout = 60 seconds

  def wrappedReceive: Receive = {
    case SaveBinary(appName, binaryType, uploadTime, jarBytes) =>
      sender ! SaveBinaryResult(Try(dao.saveBinary(appName, binaryType, uploadTime, jarBytes)))

    case DeleteBinary(appName) =>
      sender ! DeleteBinaryResult(Try(dao.deleteBinary(appName)))

    case GetApps(typeFilter) =>
      dao.getApps.map(apps => Apps(typeFilter.map(t => apps.filter(_._2._1 == t)).getOrElse(apps))).
        pipeTo(sender)

    case GetBinaryPath(appName, binType, uploadTime) =>
      sender() ! BinaryPath(dao.retrieveBinaryFile(appName, binType, uploadTime))

    case SaveContextInfo(contextInfo) =>
      dao.saveContextInfo(contextInfo)

    case GetContextInfo(id) =>
      dao.getContextInfo(id).map(ContextResponse).pipeTo(sender)

    case GetContextInfoByName(name) =>
      dao.getContextInfoByName(name).map(ContextResponse).pipeTo(sender)

    case GetContextInfos(limit, statusOpt) =>
      dao.getContextInfos(limit, statusOpt).map(ContextInfos).pipeTo(sender)

    case SaveJobInfo(jobInfo) =>
      dao.saveJobInfo(jobInfo)

    case GetJobInfos(limit) =>
      dao.getJobInfos(limit).map(JobInfos).pipeTo(sender)

    case SaveJobConfig(jobId, jobConfig) =>
      dao.saveJobConfig(jobId, jobConfig)

    case GetJobConfig(jobId) =>
      dao.getJobConfig(jobId).map(JobConfig).pipeTo(sender)

    case GetLastUploadTimeAndType(appName) =>
      sender() ! LastUploadTimeAndType(dao.getLastUploadTimeAndType(appName))

    case CleanContextJobInfos(contextName, endTime) =>
      dao.cleanRunningJobInfosForContext(contextName, endTime)
  }
}
