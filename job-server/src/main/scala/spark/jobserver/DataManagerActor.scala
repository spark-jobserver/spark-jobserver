package spark.jobserver

import akka.actor.{ActorRef, Props}
import spark.jobserver.io.DataFileDAO
import org.joda.time.DateTime
import spark.jobserver.common.akka.InstrumentedActor
import scala.collection.mutable

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
  case class Error(msg: String)

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
        if (fileDao.deleteFile(fileName)) {
          remoteCaches.remove(fileName).map { actors =>
            actors.map { _ ! JobManagerActor.DeleteData(fileName) }
          }
          Deleted
        } else {
          Error(s"Unknown file: $fileName")
        }
      }

    case DeleteAllData =>
      sender ! {
        if (fileDao.deleteAll()) {
          remoteCaches.keySet.map { fileName =>
            remoteCaches.remove(fileName).map { actors =>
              actors.map { _ ! JobManagerActor.DeleteData(fileName) }
            }
          }
          Deleted
        } else {
          Error("Unable to delete all data")
        }
      }

    case StoreData(aName, aBytes) =>
      logger.info("Storing data in file prefix {}, {} bytes", aName, aBytes.length)
      val uploadTime = DateTime.now()
      val fName = fileDao.saveFile(aName, uploadTime, aBytes)
      remoteCaches(fName) = mutable.HashSet.empty[ActorRef]
      sender ! Stored(fName)

    case RetrieveData(fileName, jobManager) =>
      logger.info("Sending data file {} to {}", fileName, jobManager.path: Any)
      try {
        sender ! Data(fileDao.readFile(fileName))
        remoteCaches(fileName) += jobManager
      } catch {
        case e: Exception =>
          sender ! Error(s"Failed to read file $fileName: $e")
      }
  }
}
