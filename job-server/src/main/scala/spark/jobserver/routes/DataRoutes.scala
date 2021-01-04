package spark.jobserver.routes

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives.{post, _}
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import spark.jobserver.DataManagerActor.{Error, StoreData, Stored, _}
import spark.jobserver.util.ResultMarshalling.ResultKey

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

  def dataRoutes(dataManager: ActorRef)(implicit ec: ExecutionContext, ShortTimeout: Timeout): Route = {
    // Get spray-json type classes for serializing Map[String, Any]

    // GET /data route returns a JSON map of the stored files and their upload time
    get { ctx =>
      import spray.json.DefaultJsonProtocol._

      val future = (dataManager ? ListData).mapTo[collection.Set[String]]
      future.flatMap { names =>
        ctx.complete(names)
      }.recoverWith {
        case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
      }
    } ~
      // DELETE /data/filename delete the given file
      delete {
        path(Segment) { filename =>
          val future = dataManager ? DeleteData(filename)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            future.flatMap {
              case Deleted => ctx.complete(StatusCodes.OK)
              case Error =>
                completeWithErrorStatus(
                  ctx, "Unable to delete data file '" + filename + "'.", StatusCodes.BadRequest)
            }.recoverWith {
              case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
            }
          }
        }
      } ~
      put {
        parameters("reset", 'sync.as[Boolean] ?) { (reset, sync) =>
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
                    case Error =>
                      completeWithErrorStatus(ctx, "Unable to delete data folder", StatusCodes.BadRequest)
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
      } ~
      // POST /data/<filename>
      post {
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
                case Error =>
                  completeWithErrorStatus(
                    ctx, "Failed to store data file '" + filename + "'.", StatusCodes.BadRequest)
              }.recoverWith {
                case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
              }
            }
          }
        }
      }
  }
}
