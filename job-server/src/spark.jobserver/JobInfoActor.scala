package spark.jobserver

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import scala.concurrent.Await
import spark.jobserver.ContextSupervisor.{GetContext, GetAdHocContext}
import spark.jobserver.io.{JobDAOActor, JobDAO}

object JobInfoActor {
  // Requests
  case class GetJobStatuses(limit: Option[Int])
  case class GetJobConfig(jobId: String)
  case class StoreJobConfig(jobId: String, jobConfig: Config)

  // Responses
  case object JobConfigStored
}

class JobInfoActor(jobDao: ActorRef, contextSupervisor: ActorRef) extends InstrumentedActor {
  import CommonMessages._
  import JobInfoActor._
  import scala.concurrent.duration._
  import scala.util.control.Breaks._
  import context.dispatcher       // for futures to work

  // Used in the asks (?) below to request info from contextSupervisor and resultActor
  implicit val ShortTimeout = Timeout(3 seconds)

  override def wrappedReceive: Receive = {
    case GetJobStatuses(limit) =>
      import akka.pattern.{ask, pipe}
      val req = (jobDao ? JobDAOActor.GetJobInfos).mapTo[JobDAOActor.JobInfos].map { jobInfos =>
        val infos = jobInfos.jobInfos.values.toSeq.sortBy(_.startTime.toString())
        if (limit.isDefined) {
          infos.takeRight(limit.get)
        }
        else {
          infos
        }
      }.pipeTo(sender)

    case GetJobResult(jobId) =>
      import akka.pattern.{ask, pipe}
      val receiver = sender()
      val req = (jobDao ? JobDAOActor.GetJobInfos).mapTo[JobDAOActor.JobInfos]
      req.map { resp =>
        val jobInfoOpt = resp.jobInfos.get(jobId)
        if (!jobInfoOpt.isDefined) {
          NoSuchJobId
        }
        else {
          val runningOrErrored = jobInfoOpt.filter { job => job.isRunning || job.isErroredOut }
          if (runningOrErrored.isDefined) {
            runningOrErrored.get
          }
          else {
            //get the context from jobInfo
            val context = jobInfoOpt.get.contextName

            val future = (contextSupervisor ? ContextSupervisor.GetResultActor(context)).mapTo[ActorRef]
            val resultActor = Await.result(future, 3 seconds)

            val jobResultFuture = (resultActor ? GetJobResult(jobId))
            val jobResult = Await.result(jobResultFuture, 3 seconds)
            jobResult
          }
        }
      }.pipeTo(receiver)

    case GetJobConfig(jobId) =>
      import akka.pattern.{ask,pipe}
      (jobDao ? JobDAOActor.GetJobConfigs).mapTo[JobDAOActor.JobConfigs].map { jobConfigs =>
        jobConfigs.jobConfigs.get(jobId).getOrElse(NoSuchJobId)
      }.pipeTo(sender)

    case StoreJobConfig(jobId, jobConfig) =>
      jobDao ! JobDAOActor.SaveJobConfig(jobId, jobConfig)
      sender ! JobConfigStored
  }
}
