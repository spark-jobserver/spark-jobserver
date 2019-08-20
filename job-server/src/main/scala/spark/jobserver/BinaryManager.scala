package spark.jobserver

import java.io.File
import java.net.URI

import akka.actor.ActorRef
import akka.util.Timeout
import spark.jobserver.io.JobDAOActor.{
  BinaryInfosForCp, BinaryNotFound, DeleteBinaryResult, GetBinaryInfosForCpFailed, SaveBinaryResult}
import spark.jobserver.io.{BinaryInfo, BinaryType, JobDAOActor, JobInfo, JobStatus}
import spark.jobserver.util.{JarUtils, NoSuchBinaryException}
import org.joda.time.DateTime
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

/** Message for storing a JAR for an application given the byte array of the JAR file */
case class StoreBinary(appName: String, binaryType: BinaryType, binBytes: Array[Byte])
case class DeleteBinary(appName: String)
/** Message requesting a listing of the available JARs */
case class ListBinaries(typeFilter: Option[BinaryType])
case class GetBinary(appName: String)
/** Message for storing one or more local Binaries based on the given map.
  *
  * @param  localBinaries    Map where the key is the appName and the value is the local path to the Binary.
  */
case class StoreLocalBinaries(localBinaries: Map[String, (BinaryType, String)])

case class GetBinaryInfoListForCp(cp: Seq[String])

// Responses
case object InvalidBinary
case object BinaryStored
case object BinaryDeleted
case class NoSuchBinary(name: String)
case class BinaryInUse(jobs: Seq[String])
case class BinaryStorageFailure(ex: Throwable)
case class BinaryDeletionFailure(ex: Throwable)
case class GetBinaryInfoListForCpFailure(ex: Throwable)
case class BinaryInfoListForCp(binaryInfoList: Seq[BinaryInfo])

object BinaryManager {
  val DELETE_TIMEOUT = 5.seconds
}

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
    (jobDao ? JobDAOActor.DeleteBinary(appName))(BinaryManager.DELETE_TIMEOUT).
      mapTo[DeleteBinaryResult].map(_.outcome)
  }

  private def getActiveJobsUsingBinary(binName: String): Future[Seq[JobInfo]] = {
    (jobDao ? JobDAOActor.GetJobsByBinaryName(
          binName, Some(JobStatus.getNonFinalStates())))(BinaryManager.DELETE_TIMEOUT)
        .mapTo[JobDAOActor.JobInfos]
        .map(_.jobInfos)
  }

  override def wrappedReceive: Receive = {
    case GetBinary(appName) =>
      val resp = (jobDao ? JobDAOActor.GetLastBinaryInfo(appName)).mapTo[JobDAOActor.LastBinaryInfo]
      resp pipeTo sender

    case ListBinaries(filterOpt) =>
      val requestor = sender
      val resp = (jobDao ? JobDAOActor.GetApps(filterOpt)).mapTo[JobDAOActor.Apps]
      resp.map { msg => msg.apps } pipeTo requestor

    case StoreLocalBinaries(localBinaries) =>
      val successF =
        localBinaries.foldLeft(Future.successful[Boolean](true)) { (succ, pair) =>
         succ.flatMap{s =>
           if (!s) {
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
      val recipient = sender()
      logger.info(s"Deleting binary $appName")
      getActiveJobsUsingBinary(appName).onComplete {
        case Success(Seq()) =>
          logger.info(s"No active job found for binary $appName. Deleting binary.")
          deleteBinary(appName).map {
            case Success(_) => {
              BinaryDeleted
            }
            case Failure(ex) => ex match {
              case _: NoSuchBinaryException => NoSuchBinary(appName)
              case _ => BinaryDeletionFailure(ex)
            }
          }.pipeTo(recipient)
        case Success(jobs) => recipient ! BinaryInUse(jobs.map(_.jobId))
        case Failure(ex) => recipient ! BinaryDeletionFailure(ex)
      }

    case GetBinaryInfoListForCp(classPath: Seq[String]) =>
      val recipient = sender()
      (jobDao ? JobDAOActor.GetBinaryInfosForCp(classPath)) onComplete {
        case Success(BinaryInfosForCp(binInf)) => recipient ! BinaryInfoListForCp(binInf)
        case Success(BinaryNotFound(name)) =>
          logger.warn(s"Could not find binary: $name")
          recipient ! NoSuchBinary(name)
        case Success(GetBinaryInfosForCpFailed(ex)) =>
          logger.error(s"Could not get list of cp path binaries from DAOActor: ${ex.getMessage}")
          recipient ! GetBinaryInfoListForCpFailure(ex)
        case Success(message) =>
          logger.error(s"Got unknown message type in response: ${message}")
          recipient ! GetBinaryInfoListForCpFailure(new Exception("DAO returned unknown message type"))
        case Failure(ex) =>
          logger.error(s"Could not get list of cp path binaries from DAOActor: ${ex.getMessage}")
          recipient ! GetBinaryInfoListForCpFailure(ex)
      }
  }
}
