package spark.jobserver

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.io.{JobDAO, JobInfo}
import scala.concurrent.Await

import spark.jobserver.context.JavaToScalaWrapper

object JobInfoActor {
  implicit class JavaJob2Scala[C, D, R](j: JavaSparkJob[C, D, R]) {
    def toSparkJobBase: api.SparkJobBase = new JavaToScalaWrapper(j)
  }
  // Requests
  case class GetJobStatuses(limit: Option[Int], statusOpt: Option[String] = None)
  case class GetJobConfig(jobId: String)
  case class GetJobStatus(jobId: String)
  case class StoreJobConfig(jobId: String, jobConfig: Config)

  // Responses
  case object JobConfigStored
}

class JobInfoActor(jobDao: JobDAO, contextSupervisor: ActorRef) extends InstrumentedActor {
  import CommonMessages._
  import JobInfoActor._
  import context.dispatcher
  import JobInfoActor.JavaJob2Scala

  import scala.concurrent.duration._
  import scala.util.control.Breaks._       // for futures to work

  // Used in the asks (?) below to request info from contextSupervisor and resultActor
  implicit val ShortTimeout = Timeout(3 seconds)

  override def wrappedReceive: Receive = {
    case GetJobStatuses(limit, statusOpt) =>
      val originator = sender
      jobDao.getJobInfos(limit.get, statusOpt).foreach(originator ! _)

    case GetJobStatus(jobId) =>
      val originator = sender

      jobDao.getJobInfo(jobId).collect {
        case Some(jobInfo) => originator ! jobInfo
        case None          => originator ! NoSuchJobId
      }

    case GetJobResult(jobId) =>
      val originator = sender

      jobDao.getJobInfo(jobId).collect {
        case Some(jobInfo) =>
          if (jobInfo.isRunning || jobInfo.isErroredOut) {
            originator ! jobInfo
          } else {
            // get the context from jobInfo
            val context = jobInfo.contextName
            for {
              resultActor <- (contextSupervisor ? ContextSupervisor.GetResultActor(context)).mapTo[ActorRef]
              result <- resultActor ? GetJobResult(jobId) } {
              originator ! result   // a JobResult(jobId, result) object is sent
            }
          }
        case None => originator ! NoSuchJobId
      }

    case GetJobConfig(jobId) =>
      val configs = Await.result(jobDao.getJobConfigs, 60 seconds)
      sender ! configs.getOrElse(jobId, NoSuchJobId)

    case StoreJobConfig(jobId, jobConfig) =>
      jobDao.saveJobConfig(jobId, jobConfig)
      sender ! JobConfigStored
  }
}
