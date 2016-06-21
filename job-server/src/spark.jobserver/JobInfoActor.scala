package spark.jobserver

import scala.concurrent.Await

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.io.{JobDAO, JobInfo}

object JobInfoActor {
  // Requests
  case class GetJobStatuses(limit: Option[Int])
  case class GetJobConfig(jobId: String)
  case class GetJobStatus(jobId: String)
  case class StoreJobConfig(jobId: String, jobConfig: Config)

  // Responses
  case object JobConfigStored
}

class JobInfoActor(jobDao: JobDAO, contextSupervisor: ActorRef) extends InstrumentedActor {
  import scala.concurrent.duration._
  import scala.util.control.Breaks._

  import CommonMessages._
  import JobInfoActor._
  import context.dispatcher       // for futures to work

  // Used in the asks (?) below to request info from contextSupervisor and resultActor
  implicit val ShortTimeout = Timeout(3 seconds)

  override def wrappedReceive: Receive = {
    case GetJobStatuses(limit) =>
      sender ! jobDao.getJobInfos(limit.get)

    case GetJobStatus(jobId) =>
      val jobInfo = Await.result(jobDao.getJobInfo(jobId), 60 seconds)
      val resp = if (jobInfo.nonEmpty) NoSuchJobId else jobInfo.get
      sender ! resp

    case GetJobResult(jobId) =>
      breakable {
        val jobInfo: Option[JobInfo] = Await.result(jobDao.getJobInfo(jobId), 60 seconds)

        if (jobInfo.nonEmpty) {
          sender ! NoSuchJobId
          break
        }

        jobInfo.filter { job => job.isRunning || job.isErroredOut }
          .foreach { jobInfo =>
            sender ! jobInfo
            break
          }

        // get the context from jobInfo
        val context = jobInfo.get.contextName

        val future = (contextSupervisor ? ContextSupervisor.GetResultActor(context)).mapTo[ActorRef]
        val resultActor = Await.result(future, 3 seconds)

        val receiver = sender // must capture the sender since callbacks are run in a different thread
        for (result <- resultActor ? GetJobResult(jobId)) {
          receiver ! result // a JobResult(jobId, result) object is sent
        }
      }

    case GetJobConfig(jobId) =>
      val configs = Await.result(jobDao.getJobConfigs, 60 seconds)
      sender ! configs.getOrElse(jobId, NoSuchJobId)

    case StoreJobConfig(jobId, jobConfig) =>
      jobDao.saveJobConfig(jobId, jobConfig)
      sender ! JobConfigStored
  }
}