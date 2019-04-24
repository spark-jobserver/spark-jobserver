package spark.jobserver

import akka.actor.Props
import com.typesafe.config.Config
import com.yammer.metrics.core.Counter
import org.joda.time.DateTime
import spark.jobserver.MigrationActor._
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.common.akka.metrics.YammerMetrics
import spark.jobserver.io._
import spark.jobserver.JobManagerActor.ContextTerminatedException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object MigrationActor {
  case class SaveBinaryInfoH2(appName: String, binaryType: BinaryType,
                              uploadTime: DateTime, jarBytes: Array[Byte])
  case class DeleteBinaryInfoH2(appName: String)

  case class SaveJobInfoH2(jobInfo: JobInfo)
  case class SaveJobConfigH2(jobId: String, jobConfig: Config)
  case class SaveContextInfoH2(contextInfo: ContextInfo)
  case class CleanContextJobInfosInH2(contextId: String, endTime: DateTime)

  def props(config: Config): Props = Props(classOf[MigrationActor], config)
}

/**
  * @param config configuration file of jobserver
  */
class MigrationActor(config: Config) extends InstrumentedActor
  with YammerMetrics{

  val dao = new MetaDataSqlDAO(config)

  val totalLiveRequests: Counter = counter("live-total-count")

  val totalLiveSuccessfulSaveBinInfoRequests: Counter = counter("live-total-save-binary-info-success")
  val totalLiveFailedSaveBinInfoRequests: Counter = counter("live-total-save-binary-info-failed")

  val totalLiveSuccessfulDeleteBinInfoRequests: Counter = counter("live-total-delete-binary-info-success")
  val totalLiveFailedDeleteBinInfoRequests: Counter = counter("live-total-delete-binary-info-failed")
  val totalLiveDeleteNotFoundSaveBinInfoRequests: Counter =
    counter("live-total-delete-binary-info-failed-not-found")

  val totalLiveSuccessfulSaveJobRequests: Counter = counter("live-total-save-job-success")
  val totalLiveFailedSaveJobRequests: Counter = counter("live-total-save-job-failed")

  val totalLiveSuccessfulSaveConfigRequests: Counter = counter("live-total-save-config-success")
  val totalLiveFailedSaveConfigRequests: Counter = counter("live-total-save-config-failed")

  val totalLiveSuccessfulSaveContextRequests: Counter = counter("live-total-save-context-success")
  val totalLiveFailedSaveContextRequests: Counter = counter("live-total-save-context-failed")

  val liveRequestHandlers: Receive = {
    case SaveBinaryInfoH2(name, binaryType, uploadTime, binaryBytes) =>
      totalLiveRequests.inc()
      val binHash = BinaryDAO.calculateBinaryHashString(binaryBytes)
      logger.info(s"Saving binary info for name: $name with hash $binHash")
      dao.saveBinary(name, binaryType, uploadTime, binHash) onComplete {
        case Success(true) =>
          totalLiveSuccessfulSaveBinInfoRequests.inc()
        case Success(false) =>
          logger.info(s"Failed to save binary info for name: $name")
          totalLiveFailedSaveBinInfoRequests.inc()
        case Failure(t) =>
          logger.error(s"Got exception trying to save binary info for name: $name", t)
          totalLiveFailedSaveBinInfoRequests.inc()
      }

    case DeleteBinaryInfoH2(name) =>
      totalLiveRequests.inc()
      val binary = dao.getBinary(name)
      binary onComplete {
        case Success(Some(_)) =>
          logger.info(s"Deleting binary info for name: $name")
          dao.deleteBinary(name) onComplete {
            case Success(true) =>
              totalLiveSuccessfulDeleteBinInfoRequests.inc()
            case Failure(t) =>
              logger.error(s"Got exception trying to delete binary info for name: $name", t)
              totalLiveFailedDeleteBinInfoRequests.inc()
            case _ =>
              logger.info(s"Failed to delete binary info for name: $name")
              totalLiveFailedDeleteBinInfoRequests.inc()
            }
        case _ =>
          logger.info(s"Didn't find binary info for name: $name")
          totalLiveDeleteNotFoundSaveBinInfoRequests.inc()
          totalLiveFailedDeleteBinInfoRequests.inc()
      }

    case SaveContextInfoH2(contextInfo) =>
      totalLiveRequests.inc()
      logger.info(s"Saving context info with: ${contextInfo.id}")
      dao.saveContext(contextInfo) onComplete {
        case Success(true) =>
          totalLiveSuccessfulSaveContextRequests.inc()
        case Success(false) =>
          logger.info(s"Failed to save context info for id: ${contextInfo.id}")
          totalLiveFailedSaveContextRequests.inc()
        case Failure(t) =>
          logger.error(s"Got exception trying to save context info for id: ${contextInfo.id}", t)
          totalLiveFailedSaveContextRequests.inc()
      }


    case SaveJobConfigH2(jobId, jobConfig) =>
      totalLiveRequests.inc()
      logger.info(s"Saving job config with id: $jobId")
      dao.saveJobConfig(jobId, jobConfig) onComplete {
        case Success(true) =>
          totalLiveSuccessfulSaveConfigRequests.inc()
        case Success(false) =>
          logger.info(s"Failed to save job config with id: $jobId")
          totalLiveFailedSaveConfigRequests.inc()
        case Failure(t) =>
          logger.error(s"Got exception trying to save job config with id: $jobId", t)
          totalLiveFailedSaveConfigRequests.inc()
      }

    case SaveJobInfoH2(jobInfo) =>
      totalLiveRequests.inc()
      logger.info(s"Saving job info with id: ${jobInfo.jobId}")
      dao.saveJob(jobInfo) onComplete {
        case Success(true) =>
          totalLiveSuccessfulSaveJobRequests.inc()
        case Success(false) =>
          logger.info(s"Failed to save job info with id: ${jobInfo.jobId}")
          totalLiveFailedSaveJobRequests.inc()
        case Failure(t) =>
          logger.error(s"Got exception trying to save job info with id: ${jobInfo.jobId}", t)
          totalLiveFailedSaveJobRequests.inc()
      }

    // This function uses H2 DAO to pull all the jobs in non-final state instead of ZK dao because
    // if migrationActor also uses ZK dao then it can happen that we don't get any non-final jobs because
    // the saveJob function is already executed in JobDaoActor.
    case CleanContextJobInfosInH2(contextId, endTime) =>
      logger.info(s"Cleanup: Cleaning jobs for context $contextId")
      dao.getJobsByContextId(contextId, Some(JobStatus.getNonFinalStates())).map { infos =>
        logger.info(s"Cleanup: Found jobs to cleanup: ${infos.map(_.jobId).mkString(", ")}")
        for (info <- infos) {
          val updatedInfo = info.copy(
            state = JobStatus.Error,
            endTime = Some(endTime),
            error = Some(ErrorData(ContextTerminatedException(contextId))))
          logger.info(s"Cleanup: Sending save job info message to self. Job id is ${updatedInfo.jobId}")

          // Sending message to self instead of dao.saveJob() to update timer + counters
          self ! SaveJobInfoH2(updatedInfo)
        }
      }
  }

  override def wrappedReceive: Receive = liveRequestHandlers
}
