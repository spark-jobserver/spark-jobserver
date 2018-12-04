package spark.jobserver

import java.io._
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Cancellable, Props}
import com.typesafe.config.Config
import spark.jobserver.MigrationActor._
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.common.akka.metrics.YammerMetrics
import spark.jobserver.io.{BinaryDAO, HdfsBinaryDAO}
import akka.pattern.ask
import com.yammer.metrics.Metrics
import org.apache.commons.configuration.PropertiesConfiguration

import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import org.apache.commons.configuration.ConfigurationException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

object MigrationActor {
  case class SaveBinaryInHDFS(name: String, jarBytes: Array[Byte])
  case class DeleteBinaryFromHDFS(name: String)
  case object Init
  case object ProcessNextBinary
  case object StartScheduler

  case object GetAllHashes
  sealed trait GetAllHashesResponse
  case class GetAllHashesSucceeded(hash: Seq[String]) extends GetAllHashesResponse
  case object GetAllHashesFailed extends GetAllHashesResponse

  case class GetHashForApp(appName: String)
  sealed trait GetHashForAppResponse
  case class GetHashForAppSucceeded(hash: Seq[String]) extends GetHashForAppResponse
  case object GetHashForAppFailed extends GetHashForAppResponse

  case class GetBinary(hash: String)
  sealed trait GetBinaryReponse
  case class GetBinarySucceeded(binary: Array[Byte]) extends GetBinaryReponse
  case object GetBinaryFailed extends GetBinaryReponse

  def props(config: Config,
            daoActor: ActorRef,
            initRetry: FiniteDuration = 30.seconds,
            syncInterval: FiniteDuration = 1.minute,
            autoStartSync: Boolean = true): Props =
    Props(classOf[MigrationActor], config, daoActor, initRetry, syncInterval, autoStartSync)
}

/**
  *
  * @param config The configuration file of jobserver
  * @param daoActor The dao actor to fetch binaries
  * @param initRetryTimeInterval Time interval for fetching hashes from dao
  * @param syncInterval Interval for syncing binaries (default 1 minute)
  * @param autoStartSync true, if syncing should start immediately
  */
class MigrationActor(config: Config,
                     daoActor: ActorRef,
                     initRetryTimeInterval: FiniteDuration,
                     syncInterval: FiniteDuration,
                     autoStartSync: Boolean)
    extends InstrumentedActor with YammerMetrics {
  val hdfsDAO = new HdfsBinaryDAO(config)

  val metadataStore = new MigrationMetaData(config.getString("spark.jobserver.sqldao.rootdir"))

  val totalLiveRequests = counter("live-total-count")
  val totalLiveSuccessfulSaveRequests = counter("live-total-save-binary-success")
  val totalLiveFailedSaveRequests = counter("live-total-save-binary-failed")

  val totalLiveSuccessfulDeleteRequests = counter("live-total-delete-binary-success")
  val totalLiveFailedDeleteRequests = counter("live-total-delete-binary-failed")

  val totalSyncCount = counter("sync-total-count")
  val totalSuccessfulSyncRequests = counter("sync-total-success")
  val totalFailedSyncRequests = counter("sync-total-failed")
  val totalDeletedBinarySyncRequests = counter("sync-total-deleted")

  val syncSaveTimer = Metrics.newTimer(getClass, "sync-save-timer",
    TimeUnit.MILLISECONDS, TimeUnit.SECONDS)

  var syncCancellable: Cancellable = _

  if (autoStartSync) {
    context.system.scheduler.scheduleOnce(3.seconds, self, Init)
  }

  val syncHandlers: Receive = {
    case Init =>
      if (metadataStore.exists()) {
        logger.info("Sync file already exists, just starting scheduler")
        totalSyncCount.inc(metadataStore.totalKeys)
        totalSuccessfulSyncRequests.inc(metadataStore.getCurrentIndex.toInt - 1)
        self ! StartScheduler
      } else {
        daoActor ! GetAllHashes
      }

    case GetAllHashesFailed =>
      logger.error("Failed to fetch hashes. Retrying in 30 seconds (default) ...")
      context.system.scheduler.scheduleOnce(initRetryTimeInterval, daoActor, GetAllHashes)

    case GetAllHashesSucceeded(hashes) =>
      logger.info(s"Received hashes for all the binaries. Total ${hashes.length}")
      totalSyncCount.inc(hashes.length)

      metadataStore.save(hashes) match {
        case true =>
          logger.info("Saved successfully all the hashes to config file")
          self ! StartScheduler
        case false =>
          logger.error("Failed to save hashes to config file. Retrying in 30 seconds ...")
          context.system.scheduler.scheduleOnce(30.seconds, self, GetAllHashesSucceeded(hashes))
      }

    case StartScheduler =>
      val isScheduled = startSyncScheduler
      isScheduled match {
        case false =>
          // Retry after 30 seconds, even though the chances of this failing is too low
          context.system.scheduler.scheduleOnce(initRetryTimeInterval, self, StartScheduler)
        case true =>
          logger.info("Successfully started scheduler")
      }


    case ProcessNextBinary =>
      val hashOption = metadataStore.getNext()
      hashOption match {
        case Some(binHash) =>
          logger.info(s"Fetching next binary with hash $binHash")
          (daoActor ? GetBinary(binHash))(10.seconds).mapTo[GetBinaryReponse].onComplete {
            case Success(GetBinarySucceeded(Array.emptyByteArray)) =>
              logger.warn(s"Found a hash ($binHash) which doesn't exist in DB")
              metadataStore.updateIndex()
              totalDeletedBinarySyncRequests.inc()
            case Success(GetBinarySucceeded(bytes)) =>
              saveSyncBinary(binHash, bytes)
            case Success(GetBinaryFailed) =>
              logger.error("Received GetBinaryFailed. Failed")
              totalFailedSyncRequests.inc()
            case Failure(t) =>
              logger.error(s"Failed to get binary $binHash", t)
              totalFailedSyncRequests.inc()
          }
        case None =>
          logger.info("All binaries synced successfully.")
          syncCancellable.cancel()
      }
  }

  val liveRequestHandlers: Receive = {
    case SaveBinaryInHDFS(name, binaryBytes) =>
      totalLiveRequests.inc()
      val binHash = BinaryDAO.calculateBinaryHashString(binaryBytes)
      hdfsDAO.save(binHash, binaryBytes).onComplete {
        case Success(true) =>
          logger.info(s"Successfully saved binary with details $name/$binHash")
          totalLiveSuccessfulSaveRequests.inc()
        case Success(false) =>
          logger.info(s"Failed to save binary with details $name/$binHash")
          totalLiveFailedSaveRequests.inc()
        case Failure(t) =>
          logger.error(s"Failed to save binary with details $name/$binHash", t)
          totalLiveFailedSaveRequests.inc()
      }

    /**
      * This function return back "Proceed" to let WebAPI know that it has
      * read necessary information to delete data from HDFS.
      *
      * If we don't do this, then delete binary wipes the data from DB and when
      * this actor tries to read the data, it is not there.
      */
    case DeleteBinaryFromHDFS(name) =>
      totalLiveRequests.inc()
      val recipient = sender()
      (daoActor ? GetHashForApp(name))(3.seconds)
          .mapTo[GetHashForAppResponse].onComplete {
        case Success(GetHashForAppSucceeded(Seq())) =>
          logger.warn(s"Did not find any hashes for app $name")
          totalLiveSuccessfulDeleteRequests.inc()
          recipient ! "Proceed"
        case Success(GetHashForAppSucceeded(hashes)) =>
          deleteBinariesFromHDFS(hashes)
          recipient ! "Proceed"
        case Success(GetHashForAppFailed) =>
          logger.error("Received GetAllHashesForAppFailed for app $name")
          totalLiveFailedDeleteRequests.inc()
          recipient ! "Proceed"
        case Failure(t) =>
          logger.error(s"Failed to get hashes for app $name", t)
          totalLiveFailedDeleteRequests.inc()
          recipient ! "Proceed"
      }
  }

  override def wrappedReceive: Receive = syncHandlers.orElse(liveRequestHandlers)

  private def saveSyncBinary(binHash: String, bytes: Array[Byte]) = {
    val timer = syncSaveTimer.time()
    hdfsDAO.save(binHash, bytes).onComplete {
      case Success(true) =>
        logger.info(s"Successfully saved binary with details $binHash")
        totalSuccessfulSyncRequests.inc()
        metadataStore.updateIndex()
        timer.stop()
      case Success(false) =>
        logger.info(s"Failed to save binary with details $binHash")
        totalFailedSyncRequests.inc()
        timer.stop()
      case Failure(t) =>
        logger.error(s"Failed to save binary with details $binHash", t)
        totalFailedSyncRequests.inc()
        timer.stop()
    }
  }

  private def deleteBinariesFromHDFS(hashes: Seq[String]): Unit = {
    hashes.foreach{
      h => hdfsDAO.delete(h).onComplete {
        case Success(true) =>
          logger.info(s"Successfully deleted binary with hash $h")
          totalLiveSuccessfulDeleteRequests.inc()
        case Success(false) =>
          logger.error(s"Failed to delete binary with hash $h")
          totalLiveFailedDeleteRequests.inc()
        case Failure(t) =>
          logger.error("Delete request for hdfs dao failed", t)
          totalLiveFailedDeleteRequests.inc()
      }
    }
  }

  private def startSyncScheduler: Boolean = {
    val INITIAL_DELAY = syncInterval
    Try(context.system.scheduler.schedule(INITIAL_DELAY, syncInterval, self, ProcessNextBinary)) match {
      case Success(cancellable) =>
        logger.info("Scheduled a time out message for processing next binary")
        syncCancellable = cancellable
        true
      case Failure(e) =>
        logger.error("Failed to schedule message to process next binary", e)
        false
    }
  }
}

object MigrationMetaData {
  val FILE_NAME = "db_sync_hash_list"
  val CURRENT_KEY = "current_index"
  val TOTAL_KEYS = "total_keys"
}

/**
  * Helper class to store all the hashes with indexes.
  * getNext() will return the next hash from the config file.
  * User needs to update the index manually to move to the next hash.
  *
  * CURRENT_KEY represents the hash will needs to be processed next.
  * @param rootDir The location to store config file
  */
class MigrationMetaData(rootDir: String) {
  val logger = LoggerFactory.getLogger(getClass)

  val configurationFile = new File(rootDir, MigrationMetaData.FILE_NAME)
  val config = new PropertiesConfiguration(configurationFile)

  if (!config.containsKey(MigrationMetaData.CURRENT_KEY)) {
    config.setProperty(MigrationMetaData.CURRENT_KEY, "1")
  }

  def save(hashes: Seq[String]): Boolean = {
    try {
      config.setProperty(MigrationMetaData.TOTAL_KEYS, hashes.length.toString)
      hashes.zipWithIndex.foreach {
        case (hash, index) =>
          config.setProperty((index + 1).toString, hash)
      }
      config.save()
      true
    } catch {
      case e: ConfigurationException =>
        logger.error("Failed to save the hashes", e)
        false
    }
  }

  def getNext(): Option[String] = {
    if (allConsumed) {
      return None
    }
    Some(config.getString(getCurrentIndex))
  }

  def updateIndex(): Unit = {
    config.setProperty(MigrationMetaData.CURRENT_KEY, (getCurrentIndex.toInt + 1).toString)
    config.save()
  }

  def allConsumed: Boolean = {
    !((getCurrentIndex.toInt) <= config.getInt(MigrationMetaData.TOTAL_KEYS))
  }

  def exists(): Boolean = configurationFile.exists()

  def totalKeys: Int =
    try {
      config.getInt(MigrationMetaData.TOTAL_KEYS)
    } catch {
      case _: NoSuchElementException =>
        logger.warn("Did not find total_keys key in sync file")
        0
    }

  def getCurrentIndex: String = config.getString(MigrationMetaData.CURRENT_KEY)
}