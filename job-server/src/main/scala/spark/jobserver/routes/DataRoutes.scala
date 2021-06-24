package spark.jobserver.routes

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives.{post, _}
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import spark.jobserver.DataManagerActor.{Error, StoreData, Stored, _}
import spark.jobserver.auth.AuthInfo
import spark.jobserver.auth.Permissions._
import spark.jobserver.util.ResultMarshalling.ResultKey
import spark.jobserver.util.DirectoryException

import java.nio.file.{DirectoryNotEmptyException, FileAlreadyExistsException, NoSuchFileException}
import scala.concurrent.ExecutionContext

/**
 * Routes for listing, deletion of and storing data files
 *    GET /data                     - lists all currently stored files
 *    DELETE /data/<filename>       - deletes given file, no-op if file does not exist
 *    POST /data/<filename-prefix>  - upload a new data file, using the given prefix,
 *                                      a time stamp is appended to ensure uniqueness
 * @author TimMaltGermany
 */
trait DataRoutes extends SprayJsonSupport {
  import spark.jobserver.WebApi._

  def dataRoutes(dataManager: ActorRef, authInfo: AuthInfo)
                (implicit ec: ExecutionContext, ShortTimeout: Timeout): Route = {
    // Get spray-json type classes for serializing Map[String, Any]

    // GET /data route returns a JSON map of the stored files and their upload time
    (get & authorize(authInfo.hasPermission(DATA_READ))) { ctx =>
      import spray.json.DefaultJsonProtocol._

      val future = (dataManager ? ListData).mapTo[collection.Set[String]]
      future.flatMap { names =>
        ctx.complete(names)
      }.recoverWith {
        case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
      }
    } ~
      // DELETE /data/filename delete the given file
      (delete & authorize(authInfo.hasPermission(DATA_DELETE))) {
        path(Segment) { filename =>
          val future = dataManager ? DeleteData(filename)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            future.flatMap {
              case Deleted => ctx.complete(StatusCodes.OK)
              case Error(ex) =>
                ex match {
                  case ex: NoSuchFileException =>
                    completeWithException(ctx, "ERROR", StatusCodes.NotFound, ex)
                  case ex @ (_: DirectoryException | _: DirectoryNotEmptyException | _: SecurityException) =>
                    completeWithException(ctx, "ERROR", StatusCodes.BadRequest, ex)
                  case ex =>
                    completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, ex)
                }
            }.recoverWith {
              case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
            }
          }
        }
      } ~
      put {
        parameters("reset", 'sync.as[Boolean] ?) { (reset, sync) =>
          authorize(authInfo.hasPermission(DATA_RESET)) {
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              reset match {
                case "reboot" => {
                  if (sync.isDefined && !sync.get) {
                    dataManager ! DeleteAllData
                    completeWithSuccess(ctx, StatusCodes.OK, "Data reset requested")
                  }
                  else {
                    val future = dataManager ? DeleteAllData
                    future.flatMap {
                      case Deleted => completeWithSuccess(ctx, StatusCodes.OK, "Data reset")
                      case Error(ex) =>
                        completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, ex)
                    }.recoverWith {
                      case e: Exception =>
                        completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
                    }
                  }
                }
                case _ => ctx.complete("ERROR")
              }
            }
          }
        }
      } ~
      // POST /data/<filename>
      (post & authorize(authInfo.hasPermission(DATA_UPLOAD))) {
        path(Segment) { filename =>
          entity(as[Array[Byte]]) { bytes =>
            val future = dataManager ? StoreData(filename, bytes)
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              import spark.jobserver.common.akka.web.JsonUtils.AnyJsonFormat
              import spray.json.DefaultJsonProtocol._

              future.flatMap {
                case Stored(filename) => {
                  ctx.complete(StatusCodes.OK, Map[String, Any](
                    ResultKey -> Map("filename" -> filename)))
                }
                case Error(ex) =>
                  ex match {
                    case ex @ (_: DirectoryException | _: DirectoryNotEmptyException |
                         _: SecurityException | _: FileAlreadyExistsException) =>
                      completeWithException(ctx, "ERROR", StatusCodes.BadRequest, ex)
                    case ex =>
                      completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, ex)
                  }
              }.recoverWith {
                case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
              }
            }
          }
        }
      }
  }
}
