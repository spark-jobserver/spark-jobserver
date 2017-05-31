package spark.jobserver

import akka.actor.ActorRef
import akka.util.Timeout
import spark.jobserver.io.JobDAOActor.{DeleteBinaryResult, SaveBinaryResult}
import spark.jobserver.io.{BinaryType, JobDAOActor}
import spark.jobserver.util.JarUtils
import org.joda.time.DateTime
import java.nio.file.{Files, Paths}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

// Messages to JarManager actor

/** Message for storing a JAR for an application given the byte array of the JAR file */
case class StoreBinary(appName: String, binaryType: BinaryType, binBytes: Array[Byte])

case class DeleteBinary(appName: String)

/** Message requesting a listing of the available JARs */
case class ListBinaries(typeFilter: Option[BinaryType])

/** Message for storing one or more local Binaries based on the given map.
  *
  * @param  localBinaries    Map where the key is the appName and the value is the local path to the Binary.
  */
case class StoreLocalBinaries(localBinaries: Map[String, (BinaryType, String)])

// Responses
case object InvalidBinary
case object BinaryStored
case object BinaryDeleted
case class BinaryStorageFailure(ex: Throwable)

/**
 * An Actor that manages the jars stored by the job server.   It's important that threads do not try to
 * load a class from a jar as a new one is replacing it, so using an actor to serialize requests is perfect.
 */
class BinaryManager(jobDao: ActorRef) extends InstrumentedActor {
  import scala.concurrent.duration._
  import akka.pattern.{ask, pipe}
  import context.dispatcher
  implicit val daoAskTimeout = Timeout(60 seconds)

  private def saveBinary(appName: String,
                         binaryType: BinaryType,
                         binBytes: Array[Byte]): Future[Try[Unit]] = {
    val uploadTime = DateTime.now()
    (jobDao ? JobDAOActor.SaveBinary(appName, binaryType, uploadTime, binBytes)).
      mapTo[SaveBinaryResult].map(_.outcome)
  }

  private def deleteBinary(appName: String): Future[Try[Unit]] = {
    (jobDao ? JobDAOActor.DeleteBinary(appName)).
      mapTo[DeleteBinaryResult].map(_.outcome)
  }

  override def wrappedReceive: Receive = {
    case ListBinaries(filterOpt) =>
      val requestor = sender
      val resp = (jobDao ? JobDAOActor.GetApps(filterOpt)).mapTo[JobDAOActor.Apps]
      resp.map { msg => msg.apps } pipeTo requestor


    case StoreLocalBinaries(localBinaries) =>
      val successF =
        localBinaries.foldLeft(Future.successful[Boolean](true)) { (succ, pair) =>
         succ.flatMap{s =>
           if(!s) {
             Future.successful(false)
           } else {
             val (appName, (binaryType, binPath)) = pair
             val binBytes = Files.readAllBytes(Paths.get(binPath))
             logger.info("Storing jar for app {}, {} bytes", appName, binBytes.size)
             val binaryIsValid = JarUtils.binaryIsZip(binBytes)
             if(!binaryIsValid) {
               Future.successful(false)
             } else {
               saveBinary(appName, binaryType, binBytes).map {
                 case Success(_) => true
                 case Failure(_) => false
               }
             }
           }
          }
        }
      successF.map{
        case true => BinaryStored
        case false => InvalidBinary
      }.recover{case ex => BinaryStorageFailure(ex)}.pipeTo(sender)

      //(success => sender ! (if (success) { BinaryStored } else { InvalidBinary }))

    case StoreBinary(appName, binaryType, binBytes) =>
      logger.info(s"Storing binary of type ${binaryType.name} for app $appName, ${binBytes.length} bytes")
      if (!JarUtils.binaryIsZip(binBytes)) {
        sender ! InvalidBinary
      } else {
        saveBinary(appName, binaryType, binBytes).map{
          case Success(_) => BinaryStored
          case Failure(ex) => BinaryStorageFailure(ex)
        }.pipeTo(sender)
      }

    case DeleteBinary(appName) =>
      logger.info(s"Deleting binary $appName")
      deleteBinary(appName).pipeTo(sender)
  }
}
