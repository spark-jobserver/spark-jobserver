package spark.jobserver

import akka.actor.ActorRef
import akka.util.Timeout
import org.slf4j.LoggerFactory
import spark.jobserver.io.{BinaryType, JobDAOActor}
import spark.jobserver.util.{LRUCache, NoSuchBinaryException}

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask

import java.time.ZonedDateTime

class DependenciesCache(maxEntries: Int, dao: ActorRef) {
  private val cache = new LRUCache[(String, ZonedDateTime, BinaryType), String](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(60 seconds)

  def getBinaryPath(appName: String, binType: BinaryType, uploadTime: ZonedDateTime): String = {
    val binPath = cache.get((appName, uploadTime, binType))
    if (binPath.isDefined) {
      binPath.get
    } else {
      logger.debug(s"Updating cache with dependency $appName.")
      val jarPathReq = (
        dao ? JobDAOActor.GetBinaryPath(appName, binType, uploadTime)
        ).mapTo[JobDAOActor.BinaryPath]
      val downloadedBinPath = Await.result(jarPathReq, daoAskTimeout.duration).binPath
      if (downloadedBinPath.isEmpty) {
        throw NoSuchBinaryException(appName)
      }
      cache.put((appName, uploadTime, binType), downloadedBinPath)
      downloadedBinPath
    }
  }
}
