package spark.jobserver.util

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import org.slf4j.LoggerFactory
import com.typesafe.config.Config
import JsonProtocols.ContextInfoJsonFormat
import JsonProtocols.JobInfoJsonFormat
import com.google.common.annotations.VisibleForTesting
import spark.jobserver.io._
import spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
import spark.jobserver.io.zookeeper.ZookeeperUtils
import spark.jobserver.io.ContextStatus

import java.time.ZonedDateTime

trait DAOCleanup {
  def cleanup(): Boolean
}

object ZKCleanup {
  val JOBS_LIMIT = 10000
}

class ZKCleanup(config: Config) extends DAOCleanup {
  private val logger = LoggerFactory.getLogger(getClass)
  private val hugeTimeout = FiniteDuration(1, TimeUnit.MINUTES)
  val daoAskTimeout = FiniteDuration(
    config.getDuration("spark.jobserver.dao-timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  override def cleanup(): Boolean = {
    cleanUpContextsNoEndTime()
    cleanUpJobsNoEndTime()
    cleanupNonFinalJobsWithFinalContext()
  }

  private val dao = new MetaDataZookeeperDAO(config)
  private val zkUtils = new ZookeeperUtils(config)

  def cleanUpContextsNoEndTime() : Boolean = {
      logger.info("Repairing all contexts in final state with no endtime.")
      try{
        // Find all final contexts without endtime
        val contexts = Await.result(dao.getContexts(None, Some(ContextStatus.getFinalStates())),
          hugeTimeout)
          .filter(c => c.endTime.isEmpty)
        logger.warn(s"Found ${contexts.size} contexts in final state with no endtime.")
        // Set a suitable endtime
        Utils.usingResource(zkUtils.getClient) {
          client =>
            contexts.foreach( (contextInfo : ContextInfo) => {
              val path = s"${MetaDataZookeeperDAO.contextsDir}/${contextInfo.id}"
              logger.trace(s"Updating context $path")
              zkUtils.write(client, contextInfo.copy(endTime = Some(contextInfo.startTime)), path)
            })
        }
        logger.info(s"Updated endtime of ${contexts.size} contexts.")
        true
      } catch {
        case NonFatal(e) =>
          logger.error("Repair of contexts failed")
          Utils.logStackTrace(logger, e)
          false
      }
  }

  def cleanUpJobsNoEndTime() : Boolean = {
      logger.info("Repairing all jobs in final state with no endtime.")
      try{
        // Find all final jobs without endtime
        val jobs = Await.result(dao.getJobs(10000, None), hugeTimeout)
          .filter(j => JobStatus.getFinalStates().contains(j.state) && j.endTime.isEmpty)
        logger.warn(s"Found ${jobs.size} jobs in final state with no endtime.")
        // Set a suitable endtime
        Utils.usingResource(zkUtils.getClient) {
          client =>
            jobs.foreach( (jobInfo : JobInfo) => {
              val path = s"${MetaDataZookeeperDAO.jobsDir}/${jobInfo.jobId}"
              logger.trace(s"Updating job $path")
              zkUtils.write(client, jobInfo.copy(endTime = Some(jobInfo.startTime)), path)
            })
        }
        logger.info(s"Updated endtime of ${jobs.size} jobs.")
      } catch {
        case NonFatal(e) =>
          logger.error("Repair of jobs failed")
          Utils.logStackTrace(logger, e)
          return false
      }
      true
  }

  @VisibleForTesting
  def cleanupNonFinalJobsWithFinalContext(): Boolean = {
    logger.info("Cleaning all jobs whose contexts are in final state.")
    try {
      val jobs = getJobs(JobStatus.getNonFinalStates())

      Utils.usingResource(zkUtils.getClient) { client =>
        jobs.map { job =>
          isContextStateFinal(job.contextId) match {
              case true =>
                val updatedJob = job.copy(endTime = Some(ZonedDateTime.now()),
                    state = JobStatus.Error,
                    error = Some(ErrorData(NoCorrespondingContextAliveException(job.jobId))))

                logger.info(s"Context ${job.contextId} is in final state. " +
                  s"Cleaning up job ${job.jobId} from state ${job.state} to ${JobStatus.Error}")
                val path = s"${MetaDataZookeeperDAO.jobsDir}/${updatedJob.jobId}"
                zkUtils.write(client, updatedJob, path)
              case false =>
                logger.info(s"Context ${job.contextId} is in non-final state. Not cleaning jobs.")
            }
        }
        logger.info("Cleaned all non-final jobs with contexts in final state.")
      }
      true
    } catch {
      case NonFatal(e) =>
        logger.error("Failed to cleanup non-final jobs", e)
        false
    }
  }

  private def getJobs(states: Seq[String]): Seq[JobInfo] = {
    logger.info(s"Fetching jobs in state ${states.mkString(",")} for cleanup.")
    val matchingJobs = states.map { state =>
      (dao.getJobs(ZKCleanup.JOBS_LIMIT, Some(state))).mapTo[Seq[JobInfo]]
    }.flatMap { jobsByStateFuture =>
      Utils.retry(3, retryDelayInMs = 50)(Await.result(jobsByStateFuture, daoAskTimeout))
    }

    logger.info(s"Total jobs found in state ${states.mkString(",")} are ${matchingJobs.length}.")
    matchingJobs
  }

  @VisibleForTesting
  def isContextStateFinal(contextId: String): Boolean = {
    val contextInfo = Utils.retry(2, retryDelayInMs = 10) {
      Await.result(dao.getContext(contextId), hugeTimeout)
    }

    contextInfo match {
      case Some(info) => ContextStatus.getFinalStates().contains(info.state)
      case None => true
    }
  }
}
