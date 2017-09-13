package spark.jobserver.io

import java.io.{File, IOException}
import java.nio.file.{Files, Paths, StandardOpenOption}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.DataManagerActor.{Data, DeleteData, Error, RetrieveData}
import scala.collection.mutable
import scala.util.{Success, Failure}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/**
 * Requests data files from data manager if local files are not existing and caches them as local tmp file.
 */
class RemoteFileCache(jobManager: ActorRef, dataManager: ActorRef) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val cachedFiles = mutable.HashMap.empty[String, File]
  implicit val askTimeout: Timeout = Timeout(60 seconds)

  @throws(classOf[IOException])
  def getDataFile(filename: String): File = {
    val filePath = Paths.get(filename)

    if (Files exists filePath) {
      filePath.toFile

    } else if (cachedFiles contains filename) {
      cachedFiles(filename)

    } else {
      logger.info("Local data file not found, fetching file from remote actor: {}", filename)

      Await.result((dataManager ? RetrieveData(filename, jobManager)), askTimeout.duration) match {
        case Data(data) =>
          val tmpFile = File.createTempFile("remote-file-cache-", ".dat")
          Files.write(tmpFile.toPath, data, StandardOpenOption.SYNC)
          cachedFiles += (filename -> tmpFile)

        case Error(msg) =>
          throw new IOException(msg)
      }

      cachedFiles(filename)
    }
  }

  def deleteDataFile(name: String) {
    cachedFiles.remove(name).map { file =>
      logger.info("Removing local copy of {}", name)
      file.delete
    }
  }
}
