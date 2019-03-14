package spark.jobserver

import java.io._
import java.util.concurrent.TimeUnit

import akka.actor.Props
import com.typesafe.config.Config
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Counter, Timer}
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.ZookeeperMigrationActor._
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.common.akka.metrics.YammerMetrics
import spark.jobserver.io._
import spark.jobserver.io.zookeeper.MetaDataZookeeperDAO

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ZookeeperMigrationActor {
  case class SaveBinaryInfoInZK(appName: String, binaryType: BinaryType,
                                uploadTime: DateTime, jarBytes: Array[Byte])
  case class DeleteBinaryInfoFromZK(appName: String)

  case class SaveJobInfoInZK(jobInfo: JobInfo)
  case class SaveJobConfigInZK(jobId: String, jobConfig: Config)
  case class SaveContextInfoInZK(contextInfo: ContextInfo)

  case object StartDataSyncFromH2
  case object ScheduleProcessMetaDataObj
  case object ProcessMetaDataObj

  case class ProcessBinaryInfo(name: String)
  case class ProcessContextInfo(name: String)
  case class ProcessJobInfo(id: String)
  case class ImportJobsForContext(contextId: String)
  case class MigratedRelatedJobs(contextId: String)


  val binaryPrefix = "binary:"
  val contextPrefix = "context:"
  val jobPrefix = "jobs:"

  def props(config: Config, currentDao: JobDAO,
            dumpH2: Boolean = false, syncInterval: FiniteDuration = 10.seconds,
            initRetryTimeInterval: FiniteDuration = 60.seconds): Props =
    Props(classOf[ZookeeperMigrationActor], config, currentDao,
      dumpH2, syncInterval, initRetryTimeInterval, new MetaDataZookeeperDAO(config))
}

/**
  *
  * @param config configuration of jobserver
  * @param currentDao currently used implementation of JobDAO
  * @param dumpH2 if set to true, will start the process of syncing data from H2 to ZK
  * @param syncInterval interval between processing sync keys
  * @param initRetryTimeInterval interval to schedule migration start
  */
class ZookeeperMigrationActor(config: Config,
                              currentDao: JobDAO,
                              dumpH2: Boolean,
                              syncInterval: FiniteDuration,
                              initRetryTimeInterval: FiniteDuration,
                              zkDao: MetaDataZookeeperDAO
                             ) extends InstrumentedActor
  with YammerMetrics{


  // Live requests metrics
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

  // Sync DB metrics
  val totalContextsToSync: Counter = counter("zk-sync-total-contexts")
  val totalContextsToSyncSuccess: Counter = counter("zk-sync-total-contexts-success")
  val totalContextsToSyncFailure: Counter = counter("zk-sync-total-contexts-failure")
  val totalBinariesToSync: Counter = counter("zk-sync-total-binaries")
  val totalBinariesToSyncMissingInH2: Counter = counter("zk-sync-total-binaries-missing-in-h2")
  val totalBinariesToSyncSuccess: Counter = counter("zk-sync-total-binaries-success")
  val totalBinariesToSyncFailure: Counter = counter("zk-sync-total-binaries-failure")
  val totalJobsToSync: Counter = counter("zk-total-jobs-migration-start")
  val totalJobsWithoutBinary: Counter = counter("zk-sync-total-jobs-without-binary")
  val totalJobsToSyncSuccess: Counter = counter("zk-sync-total-jobs-with-context-success")
  val totalJobsToSyncFailure: Counter = counter("zk-sync-total-jobs-with-context-failure")
  val totalJobsConfigSuccess: Counter = counter("zk-sync-total-job-config-success")
  val totalJobsConfigFailure: Counter = counter("zk-sync-total-job-config-failure")
  val totalJobsWithoutConfig: Counter = counter("zk-sync-total-job-without-config")

  val liveSaveTimer: Timer = Metrics.newTimer(getClass, "live-save-timer",
    TimeUnit.MILLISECONDS, TimeUnit.SECONDS)

  val initRetriesLimit = 10
  var initRetriesDone = 0

  val metadataStore = new MigrationMetaData(config.getString("spark.jobserver.combineddao.rootdir"))

  if (dumpH2) {
    context.system.scheduler.scheduleOnce(3.seconds, self, StartDataSyncFromH2)
  }

  val syncHandlers: Receive = {
    case StartDataSyncFromH2 =>
      if (metadataStore.exists()) {
        logger.info("Migration file found. Continue from previous place.")
        val countersMap = metadataStore.get_counters_data()
        totalBinariesToSync.inc(countersMap(MigrationMetaData.TOTAL_BINARY_KEYS))
        totalContextsToSync.inc(countersMap(MigrationMetaData.TOTAL_CONTEXT_KEYS))
        totalJobsToSync.inc(countersMap(MigrationMetaData.TOTAL_JOB_KEYS))
        self ! ScheduleProcessMetaDataObj
      } else {
        val counters = for {
          binaries <- currentDao.getApps
          contexts <- currentDao.getAllContextsIds
          jobs <- currentDao.getAllJobIdsToSync
        } yield (binaries, contexts, jobs)
        counters.onComplete {
          case Success((binaries, contexts, jobs)) =>
            try {
              val binaryKeys = binaries.keys.map(name => s"$binaryPrefix$name").toSeq
              val contextKeys = contexts.map(name => s"$contextPrefix$name")
              val jobKeys = jobs.map(id => s"$jobPrefix$id")
              totalBinariesToSync.inc(binaryKeys.length)
              totalContextsToSync.inc(contextKeys.length)
              totalJobsToSync.inc(jobs.length)
              metadataStore.save_counter_data(binaryKeys.length, contextKeys.length, jobs.length)
              metadataStore.save(binaryKeys.union(contextKeys).union(jobKeys))
              logger.info("Saved all sync information into the file. Triggering migration start.")
              self ! ScheduleProcessMetaDataObj
            } catch {
              case NonFatal(e) =>
                logger.error(s"[Retry nr $initRetriesDone] Error during migration init: ${e.getMessage}")
                if (initRetriesDone < initRetriesLimit) {
                  initRetriesDone += 1
                  context.system.scheduler.scheduleOnce(initRetryTimeInterval, self, StartDataSyncFromH2)
                } else {
                  logger.error(s"Reached retries limit: $initRetriesLimit. Aborting migration.")
                }
            }
          case Failure(_) => logger.error("Failed to get sync information from H2. Cancel migration.")
        }
      }

    case ScheduleProcessMetaDataObj =>
      Try(context.system.scheduler.scheduleOnce(syncInterval, self, ProcessMetaDataObj)) match {
        case Success(_) =>
          logger.info("Scheduled a message for processing next meta data object")
        case Failure(e) =>
          logger.error("Failed to schedule message to process next meta data object", e)
          // Retry after 30 seconds, even though the chances of this failing is too low
          context.system.scheduler.scheduleOnce(initRetryTimeInterval, self, ScheduleProcessMetaDataObj)
      }

    case ProcessMetaDataObj =>
      metadataStore.getNext match {
        case Some(keyToProcess) if keyToProcess.startsWith(binaryPrefix) =>
            logger.info(s"Processing binary key $keyToProcess")
            self ! ProcessBinaryInfo(keyToProcess.stripPrefix(binaryPrefix))
        case Some(keyToProcess) if keyToProcess.startsWith(contextPrefix) =>
            logger.info(s"Processing context key $keyToProcess")
            self ! ProcessContextInfo(keyToProcess.stripPrefix(contextPrefix))
        case Some(keyToProcess) if keyToProcess.startsWith(jobPrefix) =>
          logger.info(s"Processing job key $keyToProcess")
          self ! ProcessJobInfo(keyToProcess.stripPrefix(jobPrefix))
        case None => logger.info("All data is successfully synced.")
        case Some(unknownKey) =>
          logger.error(s"Unexpected key type in migration file: $unknownKey")
          self ! ScheduleProcessMetaDataObj
      }

    case ProcessBinaryInfo(name) =>
      logger.info(s"Processing binary info $name")
      currentDao.getAllBinaryInfoForName(name) onComplete {
        case Success(Seq()) =>
          logger.info(s"No more binaries for the binary name: $name")
          totalBinariesToSyncMissingInH2.inc()
          metadataStore.updateIndex()
          self ! ScheduleProcessMetaDataObj
        case Success(binInfoList) =>
          zkDao.saveBinaryList(name, binInfoList) onComplete {
            case Success(true) =>
              metadataStore.updateIndex()
              totalBinariesToSyncSuccess.inc()
              self ! ScheduleProcessMetaDataObj
            case Success(false) =>
              logger.info(s"Failed to save binary info for name: $name")
              totalBinariesToSyncFailure.inc()
              self ! ScheduleProcessMetaDataObj
            case Failure(t) =>
              logger.error(s"Got exception trying to save binary info for name: $name", t)
              totalBinariesToSyncFailure.inc()
              self ! ScheduleProcessMetaDataObj
            }
        case Failure(t) =>
          logger.error(s"Got exception trying to save binary info: $name", t)
          totalBinariesToSyncFailure.inc()
          self ! ScheduleProcessMetaDataObj
      }

    case ProcessContextInfo(name) =>
      logger.info(s"Processing context name $name")
      currentDao.getContextInfo(name) onComplete {
        case Success(Some(contextInfo)) =>
          zkDao.saveContext(contextInfo) onComplete {
            case Success(true) =>
              totalContextsToSyncSuccess.inc()
              metadataStore.updateIndex()
              self ! ScheduleProcessMetaDataObj
            case Success(false) =>
              logger.info(s"Failed to save context info for id: ${contextInfo.id}")
              totalContextsToSyncFailure.inc()
              self ! ScheduleProcessMetaDataObj
            case Failure(t) =>
              logger.error(s"Got exception trying to save context info for id: ${contextInfo.id}", t)
              totalContextsToSyncFailure.inc()
              self ! ScheduleProcessMetaDataObj
          }
        case Success(None) =>
          logger.info(s"Found no context info for the name $name")
          totalContextsToSyncFailure.inc()
          self ! ScheduleProcessMetaDataObj
        case Failure(t) =>
          logger.error(s"Got exception trying to get context info for name: $name", t)
          totalContextsToSyncFailure.inc()
          self ! ScheduleProcessMetaDataObj
      }

    case ProcessJobInfo(id) =>
      logger.info(s"Importing job id $id")
      currentDao.getJobInfo(id) onComplete {
        case Success(None) =>
          logger.info(s"Was not able to retrieve job from H2 (probably no binary): $id")
          totalJobsWithoutBinary.inc()
          metadataStore.updateIndex()
          self ! ScheduleProcessMetaDataObj
        case Success(Some(jobInfo)) =>
          saveJobWithConfig(jobInfo) onComplete {
            case Success(true) =>
              // to avoid double counting if we have to process jobinfos again in case of failure
              totalJobsToSyncSuccess.inc()
              metadataStore.updateIndex()
              self ! ScheduleProcessMetaDataObj
            case Success(false) =>
              logger.error(s"Didn't save job: $id")
              self ! ScheduleProcessMetaDataObj
            case Failure(t) =>
              logger.error(s"Exception trying to save job with id: $id", t)
              self ! ScheduleProcessMetaDataObj
          }
        case Failure(t) =>
          logger.error(s"Exception trying to retrieve job with id: $id", t)
          self ! ScheduleProcessMetaDataObj
      }
  }

  def saveJobWithConfig(jobInfo: JobInfo): Future[Boolean] = {
      zkDao.saveJob(jobInfo) andThen {
          case Success(true) =>
            logger.debug(s"Successfully saved job ${jobInfo.jobId}")
          case Success(false) =>
            logger.info(s"Didn't save job ${jobInfo.jobId}")
            totalJobsToSyncFailure.inc()
          case t =>
            logger.error(s"Got exception trying to save job ${jobInfo.jobId}", t)
            totalJobsToSyncFailure.inc()
      } andThen {
          case Success(true) =>
              currentDao.getJobConfig(jobInfo.jobId) onComplete {
                case Success(None) =>
                  logger.info(s"No config for job ${jobInfo.jobId}")
                  totalJobsWithoutConfig.inc()
                case Success(Some(config)) =>
                  zkDao.saveJobConfig(jobInfo.jobId, config) onComplete {
                    case Success(_) => totalJobsConfigSuccess.inc()
                    case Failure(t) =>
                      logger.error(s"Got exception trying to write config for job ${jobInfo.jobId}", t)
                      totalJobsConfigFailure.inc()
                  }
                case Failure(t) =>
                  logger.error(s"Got exception trying to get config for job ${jobInfo.jobId}", t)
                  totalJobsConfigFailure.inc()
              }
          case _ =>
            logger.info(s"Skip saving config for job ${jobInfo.jobId}: smth went wrong during job save")
      }
  }

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
      zkDao.saveFindBinaryStorageIdAndSave(jobInfo) onComplete {
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

  override def wrappedReceive: Receive = liveRequestHandlers.orElse(syncHandlers)


  object MigrationMetaData {
    val FILE_NAME = "sync_db_to_zookeeper"
    val CURRENT_KEY = "current_index"
    val TOTAL_KEYS = "total_keys"
    val TOTAL_CONTEXT_KEYS = "total_context_keys"
    val TOTAL_BINARY_KEYS = "total_binary_keys"
    val TOTAL_JOB_KEYS = "total_job_keys"
  }

  /**
    * Helper class to store given list of UIDs with indexes.
    * getNext() will return the next hash from the config file.
    * User needs to update the index manually to move to the next UID.
    *
    * CURRENT_KEY represents the UID which needs to be processed next.
    * @param rootDir The location to store config file
    */
  class MigrationMetaData(rootDir: String) {
    private val logger = LoggerFactory.getLogger(getClass)

    val configurationFile = new File(rootDir, MigrationMetaData.FILE_NAME)
    val config = new PropertiesConfiguration(configurationFile)

    if (!config.containsKey(MigrationMetaData.CURRENT_KEY)) {
      config.setProperty(MigrationMetaData.CURRENT_KEY, "1")
    }

    def save(data: Seq[String]): Boolean = {
      try {
        config.setProperty(MigrationMetaData.TOTAL_KEYS, data.length.toString)
        data.zipWithIndex.foreach {
          case (entity, index) =>
            config.setProperty((index + 1).toString, entity)
        }
        config.save()
        true
      } catch {
        case e: ConfigurationException =>
          logger.error("Failed to save all the data", e)
          false
      }
    }

    def getNext: Option[String] = {
      if (allConsumed) {
        return None
      }
      logger.info(s"Retrieving key: ${config.getString(getCurrentIndex)}")
      Some(config.getString(getCurrentIndex))
    }

    def updateIndex(): Unit = {
      config.setProperty(MigrationMetaData.CURRENT_KEY, (getCurrentIndex.toInt + 1).toString)
      config.save()
    }

    def allConsumed: Boolean = {
      !(getCurrentIndex.toInt <= config.getInt(MigrationMetaData.TOTAL_KEYS))
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


    def save_counter_data(binaries: Int, contexts: Int, jobs: Int): Boolean = {
      try {
        config.setProperty(MigrationMetaData.TOTAL_BINARY_KEYS, binaries.toString)
        config.setProperty(MigrationMetaData.TOTAL_CONTEXT_KEYS, contexts.toString)
        config.setProperty(MigrationMetaData.TOTAL_JOB_KEYS, jobs.toString)
        config.save()
        true
      } catch {
        case e: ConfigurationException =>
          logger.error("Failed to save all the counters data", e)
          false
      }
    }

    def get_counters_data(): Map[String, Int] = {
      try {
        Map(
          MigrationMetaData.TOTAL_BINARY_KEYS -> config.getInt(MigrationMetaData.TOTAL_BINARY_KEYS),
          MigrationMetaData.TOTAL_CONTEXT_KEYS -> config.getInt(MigrationMetaData.TOTAL_CONTEXT_KEYS),
          MigrationMetaData.TOTAL_JOB_KEYS -> config.getInt(MigrationMetaData.TOTAL_JOB_KEYS)
        ).withDefaultValue(0)
      } catch {
        case e: ConfigurationException =>
          logger.error("Failed to save all the counters data", e)
          Map.empty[String, Int]
      }
    }
  }
}
