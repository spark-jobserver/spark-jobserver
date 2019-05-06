package spark.jobserver.io.zookeeper

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.typesafe.config.Config

import akka.actor.ActorRef
import akka.actor.Props
import akka.util.Timeout
import akka.pattern.ask
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.io.CombinedDAO
import spark.jobserver.io.ContextStatus
import spark.jobserver.io.JobDAOActor.ContextInfos
import spark.jobserver.io.JobDAOActor.GetContextInfos
import spark.jobserver.io.JobDAOActor.GetJobInfos
import spark.jobserver.io.JobDAOActor.JobInfos
import spark.jobserver.io.JobStatus
import spark.jobserver.io.zookeeper.AutoPurgeActor._
import spark.jobserver.util.Utils

object AutoPurgeActor {
  val logger = LoggerFactory.getLogger(getClass)

  val maxPurgeDuration = 2 minutes
  case object PurgeOldData
  case object PurgeComplete

  def props(config: Config, daoActor: ActorRef, age : Int): Props =
    Props(classOf[AutoPurgeActor], config, daoActor, age)

  def isEnabled(config : Config) : Boolean = {
    if (config.getString("spark.jobserver.jobdao") == "spark.jobserver.io.CombinedDAO"){
      if (config.getString(CombinedDAO.metaDataDaoPath) ==
        "spark.jobserver.io.zookeeper.MetaDataZookeeperDAO"){
        val enabled = config.getBoolean("spark.jobserver.zookeeperdao.autopurge")
        logger.info(s"Zookeeper autopurge feature is set to $enabled.")
        return enabled
      }
    }
    logger.info(s"Zookeeper autopurge feature is not configured.")
    false
  }
}

/**
 * Regularly purges old zookeeper data to maintain a steady performance
 */
class AutoPurgeActor(config: Config, daoActor: ActorRef, purgeOlderThanHours: Int)
    extends InstrumentedActor {

  private val zkUtils = new ZookeeperUtils(config)
  private val awaitDuration = maxPurgeDuration / 2
  private implicit val timeout = new Timeout(awaitDuration)

  context.system.scheduler.scheduleOnce(1 hour, self, AutoPurgeActor.PurgeOldData)

  override def wrappedReceive: Receive = {
    case PurgeOldData =>
      val recipient = sender
      logger.info(s"Purging data older than $purgeOlderThanHours hours.")
      val olderThanMillis = purgeOlderThanHours * 60 * 60 * 1000
      val now = new DateTime().getMillis

      // Purge contexts
      try {
        val contexts = Await.result((daoActor ? GetContextInfos(None, None))
            .mapTo[ContextInfos], awaitDuration).contextInfos
        val purgePaths = contexts.filter(c => ContextStatus.getFinalStates().contains(c.state))
          .filter(c => c.endTime.isDefined && c.endTime.get.getMillis < (now - olderThanMillis))
          .map(c => s"${MetaDataZookeeperDAO.contextsDir}/${c.id}")
        logger.info(s"Purging ${purgePaths.size}/${contexts.size} contexts.")
        deletePaths(purgePaths)
        logger.info(s"${purgePaths.size} contexts have been purged.")
      } catch {
        case NonFatal(e) =>
          logger.error("Querying contexts failed.")
          Utils.logStackTrace(logger, e)
      }

      // Purge jobs
      try{
        val jobs = Await.result((daoActor ? GetJobInfos(10000)).mapTo[JobInfos], awaitDuration).jobInfos
        val purgePaths = jobs.filter(j => JobStatus.getFinalStates().contains(j.state))
          .filter(j => j.endTime.isDefined && j.endTime.get.getMillis < (now - olderThanMillis))
          .map(j => s"${MetaDataZookeeperDAO.jobsDir}/${j.jobId}")
        logger.info(s"Purging ${purgePaths.size}/${jobs.size} jobs.")
        deletePaths(purgePaths)
        logger.info(s"${purgePaths.size} jobs have been purged.")
      } catch {
        case NonFatal(e) =>
          logger.error("Querying jobs failed.")
          Utils.logStackTrace(logger, e)
      }

      logger.info("Purge complete.")
      recipient ! PurgeComplete

  }

  private def deletePaths(paths : Seq[String]): Unit = {
    Utils.usingResource(zkUtils.getClient) {
      client =>
        paths.foreach( (path : String) => {
          logger.trace(s"Deleting $path")
          zkUtils.delete(client, path)
        })
    }
  }

}