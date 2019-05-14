package spark.jobserver.util

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.typesafe.config.Config

import JsonProtocols.ContextInfoJsonFormat
import JsonProtocols.JobInfoJsonFormat
import spark.jobserver.io.ContextInfo
import spark.jobserver.io.JobInfo
import spark.jobserver.io.JobStatus
import spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
import spark.jobserver.io.zookeeper.ZookeeperUtils
import spark.jobserver.io.ContextStatus

trait DAOCleanup {
  def cleanup(): Boolean
}

class ZKCleanup(config: Config) extends DAOCleanup {

  private val logger = LoggerFactory.getLogger(getClass)
  private val hugeTimeout = FiniteDuration(1, TimeUnit.MINUTES)
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  override def cleanup(): Boolean = {
    cleanUpContextsNoEndTime()
    cleanUpJobsNoEndTime()
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

}
