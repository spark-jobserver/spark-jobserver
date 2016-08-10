package spark.jobserver

import akka.actor.ActorRef
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import spark.jobserver.cache.LRUCache
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.common.akka.metrics.YammerMetrics

import scala.collection.mutable
import scala.util.Try

/**
 * It is an actor to manage results that are returned from jobs.
 *
 * TODO: support multiple subscribers for same JobID
 */
class JobResultActor extends InstrumentedActor with YammerMetrics {
  import CommonMessages._

  private val config = context.system.settings.config
  private val cacheSize = Try(config.getInt("spark.jobserver.job-result-cache-size")).getOrElse(10000)
  private val cacheConfig = ConfigFactory.empty()
    .withValue("cache-size", ConfigValueFactory.fromAnyRef(cacheSize))
  private val cache = LRUCache[String, Any](cacheConfig)
  private val subscribers = mutable.HashMap.empty[String, ActorRef] // subscribers

  // metrics
  val metricSubscribers = gauge("subscribers-size", subscribers.size)
  val metricResultCache = gauge("result-cache-size", cache.size)

  def wrappedReceive: Receive = {
    case Subscribe(jobId, receiver, events) =>
      if (events.contains(classOf[JobResult])) {
        subscribers(jobId) = receiver
        logger.info("Added receiver {} to subscriber list for JobID {}", receiver, jobId: Any)
      }

    case Unsubscribe(jobId, receiver) =>
      if (!subscribers.contains(jobId)) {
        sender ! NoSuchJobId
      } else {
        subscribers.remove(jobId)
        logger.info("Removed subscriber list for JobID {}", jobId)
      }

    case GetJobResult(jobId) =>
      //sender ! JobResult(jobId, _: Any)
      sender ! cache.getOption(jobId).map(JobResult(jobId, _)).getOrElse(NoSuchJobId)

    case JobResult(jobId, result) =>
      cache.put(jobId, result)
      logger.debug("Received job results for JobID {}", jobId)
      subscribers.get(jobId).foreach(_ ! JobResult(jobId, result))
      subscribers.remove(jobId)
  }

}
