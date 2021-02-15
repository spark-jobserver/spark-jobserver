package spark.jobserver

import akka.actor.{ActorRef, Props}
import spark.jobserver.io.DataFileDAO
import spark.jobserver.common.akka.InstrumentedActor

import java.time.ZonedDateTime
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object DataManagerActor {
  // Messages to DataManager actor
  case class StoreData(name: String, bytes: Array[Byte])
  case class RetrieveData(name: String, jobManager: ActorRef)
  case class DeleteData(name: String)
  case class DeleteAllData()
  case object ListData

  // Responses
  case class Stored(name: String)
  case class Data(bytes: Array[Byte])
  case object Deleted
  case class Error(ex: Throwable)

  def props(fileDao: DataFileDAO): Props = Props(classOf[DataManagerActor], fileDao)
}

/**
 * An Actor that manages the data files stored by the job server to disc.
 */
class DataManagerActor(fileDao: DataFileDAO) extends InstrumentedActor {
  import DataManagerActor._

  // Actors with a cached copy by filename
  private val remoteCaches = mutable.HashMap.empty[String, mutable.HashSet[ActorRef]]

  override def wrappedReceive: Receive = {
    case ListData => sender ! fileDao.listFiles

    case DeleteData(fileName) =>
      sender ! {
        Try(fileDao.deleteFile(fileName)) match {
          case Success(_) =>
            remoteCaches.remove(fileName).map { actors =>
              actors.map { _ ! JobManagerActor.DeleteData(fileName) }
            }
            Deleted
          case Failure(ex) => Error(ex)
        }
      }

    case DeleteAllData =>
      sender ! {
        Try(fileDao.deleteAll()) match {
          case Success(_) =>
            remoteCaches.keySet.map { fileName =>
              remoteCaches.remove(fileName).map { actors =>
                actors.map {
                  _ ! JobManagerActor.DeleteData(fileName)
                }
              }
            }
            Deleted
          case Failure(ex) => Error(ex)
        }
      }

    case StoreData(aName, aBytes) =>
      logger.info("Storing data in file prefix {}, {} bytes", aName, aBytes.length)
      val uploadTime = ZonedDateTime.now()
      sender ! {
        Try(fileDao.saveFile(aName, uploadTime, aBytes)) match {
          case Success(fName) =>
            remoteCaches(fName) = mutable.HashSet.empty[ActorRef]
            Stored(fName)
          case Failure(ex) => Error(ex)
        }
      }

    case RetrieveData(fileName, jobManager) =>
      logger.info("Sending data file {} to {}", fileName, jobManager.path: Any)
      sender ! {
        Try(fileDao.readFile(fileName)) match {
          case Success(file) =>
            remoteCaches(fileName) += jobManager
            Data(file)
          case Failure(ex) => Error(ex)
        }
      }
  }
}
