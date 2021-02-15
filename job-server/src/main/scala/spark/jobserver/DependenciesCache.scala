package spark.jobserver

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import spark.jobserver.io.{BinaryType, JobDAOActor}
import spark.jobserver.util.{LRUCache, NoSuchBinaryException}

import java.nio.file.Path
import scala.concurrent.Await
import scala.concurrent.duration._

import java.time.ZonedDateTime

class DependenciesCache(maxEntries: Int, dao: ActorRef) {
  private val cache = new LRUCache[(String, ZonedDateTime, BinaryType), Path](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(60 seconds)

  def getBinaryPath(appName: String, binType: BinaryType, uploadTime: ZonedDateTime): Path = {
    cache
      .get((appName, uploadTime, binType))
      .orElse({
        logger.debug(s"Updating cache with dependency $appName.")
        val jarPathReq = (
          dao ? JobDAOActor.GetBinaryPath(appName, binType, uploadTime)
          ).mapTo[JobDAOActor.BinaryPath]
        Await.result(jarPathReq, daoAskTimeout.duration)
          .binPath
          .map(p => {
            cache.put((appName, uploadTime, binType), p)
            p
          })
      })
      .getOrElse(throw NoSuchBinaryException(appName))
  }
}
