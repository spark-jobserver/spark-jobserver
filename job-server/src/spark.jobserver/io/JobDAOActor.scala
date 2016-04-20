package spark.jobserver.io

import akka.actor.Props
import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import org.joda.time.DateTime

object JobDAOActor {

  //Requests
  sealed trait JobDAORequest
  case class SaveJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]) extends JobDAORequest
  case object GetApps extends JobDAORequest
  case class GetJarPath(appName: String, uploadTime: DateTime) extends JobDAORequest

  case class SaveJobInfo(jobInfo: JobInfo) extends JobDAORequest
  case class GetJobInfos(limit: Int) extends JobDAORequest

  case class SaveJobConfig(jobId: String, jobConfig: Config) extends JobDAORequest
  case object GetJobConfigs extends JobDAORequest

  case class GetLastUploadTime(appName: String) extends JobDAORequest

  //Responses
  sealed trait JobDAOResponse
  case class Apps(apps: Map[String, DateTime]) extends JobDAOResponse
  case class JarPath(jarPath: String) extends JobDAOResponse
  case class JobInfos(jobInfos: Seq[JobInfo]) extends JobDAOResponse
  case class JobConfigs(jobConfigs: Map[String, Config]) extends JobDAOResponse
  case class LastUploadTime(lastUploadTime: Option[DateTime]) extends JobDAOResponse

  case object InvalidJar extends JobDAOResponse
  case object JarStored extends JobDAOResponse

  def props(dao: JobDAO): Props = Props(classOf[JobDAOActor], dao)
}

class JobDAOActor(dao: JobDAO) extends InstrumentedActor {
  import JobDAOActor._

  def wrappedReceive: Receive = {
    case SaveJar(appName, uploadTime, jarBytes) =>
      dao.saveJar(appName, uploadTime, jarBytes)

    case GetApps =>
      sender() ! Apps(dao.getApps)

    case GetJarPath(appName, uploadTime) =>
      sender() ! JarPath(dao.retrieveJarFile(appName, uploadTime))

    case SaveJobInfo(jobInfo) =>
      dao.saveJobInfo(jobInfo)

    case GetJobInfos(limit) =>
      sender() ! JobInfos(dao.getJobInfos(limit))

    case SaveJobConfig(jobId, jobConfig) =>
      dao.saveJobConfig(jobId, jobConfig)

    case GetJobConfigs =>
      sender() ! JobConfigs(dao.getJobConfigs)

    case GetLastUploadTime(appName) =>
      sender() ! LastUploadTime(dao.getLastUploadTime(appName))
  }
}
