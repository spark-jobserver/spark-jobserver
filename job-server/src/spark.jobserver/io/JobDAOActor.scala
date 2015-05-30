package spark.jobserver.io

import akka.actor.Props
import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import org.joda.time.DateTime
import spark.jobserver.io.JobDAOActor.SaveJar
import spark.jobserver.util.JarUtils

/**
 * Created by ankits on 4/6/15.
 */

object JobDAOActor {

  //Requests
  case class SaveJar(appName:String, uploadTime:DateTime, jarBytes:Array[Byte])
  case object GetApps
  case class GetJarPath(appName:String, uploadTime:DateTime)

  case class SaveJobInfo(jobInfo:JobInfo)
  case object GetJobInfos

  case class SaveJobConfig(jobId:String, jobConfig:Config)
  case object GetJobConfigs

  case class GetLastUploadTime(appName:String)

  //Responses
  case class Apps(apps:Map[String, DateTime]) //GetApps?
  case class JarPath(jarPath:String) //GetJarPath?
  case class JobInfos(jobInfos:Map[String,JobInfo]) //GetJobInfos?
  case class JobConfigs(jobConfigs:Map[String, Config]) //GetJobConfigs?
  case class LastUploadTime(lastUploadTime:Option[DateTime]) //GetLastUploadTime?

  case object InvalidJar //SaveJar?
  case object JarStored //SaveJar?

  def props(dao: JobDAO): Props = Props(classOf[JobDAOActor], dao)
}

class JobDAOActor(dao:JobDAO) extends InstrumentedActor {
  import JobDAOActor._

  def wrappedReceive: Receive = {
    case SaveJar(appName,uploadTime,jarBytes) =>
      dao.saveJar(appName, uploadTime, jarBytes)

    case GetApps =>
      sender() ! Apps(dao.getApps)

    case GetJarPath(appName,uploadTime) =>
      sender() ! JarPath(dao.retrieveJarFile(appName,uploadTime))

    case SaveJobInfo(jobInfo) =>
      dao.saveJobInfo(jobInfo)

    case GetJobInfos =>
      sender() ! JobInfos(dao.getJobInfos)

    case SaveJobConfig(jobId,jobConfig) =>
      dao.saveJobConfig(jobId,jobConfig)

    case GetJobConfigs =>
      sender() ! JobConfigs(dao.getJobConfigs)

    case GetLastUploadTime(appName) =>
      sender() ! LastUploadTime(dao.getLastUploadTime(appName))

  }


}
