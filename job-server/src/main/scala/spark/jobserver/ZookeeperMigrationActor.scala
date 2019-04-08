package spark.jobserver

import java.util.concurrent.TimeUnit

import akka.actor.Props
import com.typesafe.config.Config
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Counter, Timer}
import org.joda.time.DateTime
import spark.jobserver.ZookeeperMigrationActor._
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.common.akka.metrics.YammerMetrics
import spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
import spark.jobserver.io.{BinaryDAO, BinaryType, ContextInfo, JobInfo}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object ZookeeperMigrationActor {
  case class SaveBinaryInfoInZK(appName: String, binaryType: BinaryType,
                                uploadTime: DateTime, jarBytes: Array[Byte])
  case class DeleteBinaryInfoFromZK(appName: String)

  case class SaveJobInfoInZK(jobInfo: JobInfo)
  case class SaveJobConfigInZK(jobId: String, jobConfig: Config)
  case class SaveContextInfoInZK(contextInfo: ContextInfo)

  def props(config: Config): Props = Props(classOf[ZookeeperMigrationActor], config)
}

/**
  * @param config configuration file of jobserver
  */
class ZookeeperMigrationActor(config: Config) extends InstrumentedActor
  with YammerMetrics{

  val zkDao = new MetaDataZookeeperDAO(config)

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

  val liveSaveTimer: Timer = Metrics.newTimer(getClass, "live-save-timer",
    TimeUnit.MILLISECONDS, TimeUnit.SECONDS)

  val liveRequestHandlers: Receive = {
    case SaveBinaryInfoInZK(name, binaryType, uploadTime, binaryBytes) =>
      totalLiveRequests.inc()
      val binHash = BinaryDAO.calculateBinaryHashString(binaryBytes)
      val timer = liveSaveTimer.time()
      logger.info(s"Saving binary info for name: $name with hash $binHash")
      zkDao.saveBinary(name, binaryType, uploadTime, binHash) onComplete {
        case Success(true) =>
          timer.stop()
          totalLiveSuccessfulSaveBinInfoRequests.inc()
        case Success(false) =>
          timer.stop()
          logger.info(s"Failed to save binary info for name: $name")
          totalLiveFailedSaveBinInfoRequests.inc()
        case Failure(t) =>
          timer.stop()
          logger.error(s"Got exception trying to save binary info for name: $name", t)
          totalLiveFailedSaveBinInfoRequests.inc()
      }

    case DeleteBinaryInfoFromZK(name) =>
      totalLiveRequests.inc()
      val binary = zkDao.getBinary(name)
      binary onComplete {
        case Success(Some(_)) =>
          logger.info(s"Deleting binary info for name: $name")
          zkDao.deleteBinary(name) onComplete {
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

    case SaveContextInfoInZK(contextInfo) =>
      totalLiveRequests.inc()
      logger.info(s"Saving context info with: ${contextInfo.id}")
      val timer = liveSaveTimer.time()
      zkDao.saveContext(contextInfo) onComplete {
        case Success(true) =>
          timer.stop()
          totalLiveSuccessfulSaveContextRequests.inc()
        case Success(false) =>
          timer.stop()
          logger.info(s"Failed to save context info for id: ${contextInfo.id}")
          totalLiveFailedSaveContextRequests.inc()
        case Failure(t) =>
          timer.stop()
          logger.error(s"Got exception trying to save context info for id: ${contextInfo.id}", t)
          totalLiveFailedSaveContextRequests.inc()
      }


    case SaveJobConfigInZK(jobId, jobConfig) =>
      totalLiveRequests.inc()
      logger.info(s"Saving job config with id: $jobId")
      val timer = liveSaveTimer.time()
      zkDao.saveJobConfig(jobId, jobConfig) onComplete {
        case Success(true) =>
          timer.stop()
          totalLiveSuccessfulSaveConfigRequests.inc()
        case Success(false) =>
          timer.stop()
          logger.info(s"Failed to save job config with id: $jobId")
          totalLiveFailedSaveConfigRequests.inc()
        case Failure(t) =>
          timer.stop()
          logger.error(s"Got exception trying to save job config with id: $jobId", t)
          totalLiveFailedSaveConfigRequests.inc()
      }

    case SaveJobInfoInZK(jobInfo) =>
      totalLiveRequests.inc()
      logger.info(s"Saving job info with id: ${jobInfo.jobId}")
      val timer = liveSaveTimer.time()
      zkDao.saveJob(jobInfo) onComplete {
        case Success(true) =>
          timer.stop()
          totalLiveSuccessfulSaveJobRequests.inc()
        case Success(false) =>
          timer.stop()
          logger.info(s"Failed to save job info with id: ${jobInfo.jobId}")
          totalLiveFailedSaveJobRequests.inc()
        case Failure(t) =>
          timer.stop()
          logger.error(s"Got exception trying to save job info with id: ${jobInfo.jobId}", t)
          totalLiveFailedSaveJobRequests.inc()
      }

  }

  override def wrappedReceive: Receive = liveRequestHandlers

}
