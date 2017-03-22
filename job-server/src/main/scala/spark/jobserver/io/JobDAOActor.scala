package spark.jobserver.io

import akka.actor.Props
import com.typesafe.config.Config
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.util.Try

import spark.jobserver.common.akka.InstrumentedActor

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
  case class GetBinaryContent(appName: String,
                              binaryType: BinaryType,
                              uploadTime: DateTime) extends JobDAORequest

  case class SaveJobInfo(jobInfo: JobInfo) extends JobDAORequest
  case class GetJobInfos(limit: Int) extends JobDAORequest

  case class SaveJobConfig(jobId:String, jobConfig:Config) extends JobDAORequest
  case object GetJobConfigs extends JobDAORequest

  case class GetLastUploadTimeAndType(appName: String) extends JobDAORequest

  //Responses
  sealed trait JobDAOResponse
  case class Apps(apps: Map[String, (BinaryType, DateTime)]) extends JobDAOResponse
  case class BinaryPath(binPath: String) extends JobDAOResponse
  case class BinaryContent(content: Array[Byte]) extends JobDAOResponse
  case class JobInfos(jobInfos: Seq[JobInfo]) extends JobDAOResponse
  case class JobConfigs(jobConfigs: Map[String, Config]) extends JobDAOResponse
  case class LastUploadTimeAndType(uploadTimeAndType: Option[(DateTime, BinaryType)]) extends JobDAOResponse

  case object InvalidJar extends JobDAOResponse
  case object JarStored extends JobDAOResponse

  def props(dao: JobDAO): Props = Props(classOf[JobDAOActor], dao)
}

class JobDAOActor(dao:JobDAO) extends InstrumentedActor {
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

    case SaveJobInfo(jobInfo) =>
      dao.saveJobInfo(jobInfo)

    case GetJobInfos(limit) =>
      dao.getJobInfos(limit).map(JobInfos).pipeTo(sender)

    case SaveJobConfig(jobId, jobConfig) =>
      dao.saveJobConfig(jobId,jobConfig)

    case GetJobConfigs =>
      dao.getJobConfigs.map(JobConfigs).pipeTo(sender)

    case GetLastUploadTimeAndType(appName) =>
      sender() ! LastUploadTimeAndType(dao.getLastUploadTimeAndType(appName))

    case GetBinaryContent(appName, binaryType, uploadTime) =>
      sender() ! BinaryContent(dao.getBinaryContent(appName, binaryType, uploadTime))
  }
}
