package spark.jobserver.routes

import akka.http.scaladsl.marshalling.ToResponseMarshallable._
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import spark.jobserver.DataManagerActor._

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import akka.http.scaladsl.model.StatusCodes
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

import akka.http.scaladsl.server.Directives._
import spark.jobserver.common.akka.web.JsonUtils._

/**
 * Routes for listing, deletion of and storing data files
 *    GET /data                     - lists all currently stored files
 *    DELETE /data/<filename>       - deletes given file, no-op if file does not exist
 *    POST /data/<filename-prefix>  - upload a new data file, using the given prefix,
 *                                      a time stamp is appended to ensure uniqueness
 * @author TimMaltGermany
 */
trait DataRoutes {
  import spark.jobserver.WebApi._

//  implicit val mapMarshaller: ToEntityMarshaller[Map[String, Any]] = Marshaller.opaque { map =>
//    HttpEntity(ContentType(MediaTypes.`application/json`), map.toJson)
//  }

  def dataRoutes(dataManager: ActorRef)(implicit ec: ExecutionContext, ShortTimeout: Timeout): Route = {
    // Get spray-json type classes for serializing Map[String, Any]


    // GET /data route returns a JSON map of the stored files and their upload time
    get {
      onComplete((dataManager ? ListData).mapTo[collection.Set[String]]){
        case Success(names) => complete(names)
        case Failure(ex) => complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
      }
    } ~
      // DELETE /data/filename delete the given file
      delete {
        path(Segment) { filename =>
        onComplete(dataManager ? DeleteData(filename)){
          case Success(value) => value match {
            case Deleted =>
              complete(StatusCodes.OK)
            case Error =>
              complete(StatusCodes.BadRequest, errMap(s"Unable to delete data file '$filename'."))
          }
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
        }
        }
      } ~
      put {
        parameters("reset", 'sync.as[Boolean] ?) { (reset, sync) =>
            reset match {
              case "reboot" =>
                if (sync.isDefined && !sync.get) {
                  dataManager ! DeleteAllData
                  complete(StatusCodes.OK, successMap("Data reset requested"))
                }
                else {
                  onComplete(dataManager ? DeleteAllData) {
                    case Success(value) => value match {
                      case Deleted =>
                        complete(StatusCodes.OK, successMap("Data reset"))

                      case Error =>
                        complete(StatusCodes.BadRequest, errMap("Unable to delete data folder"))
                    }
                    case Failure(ex) =>
                      complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
                  }
                }
              case _ => complete("ERROR")
            }
          }
        }
      } ~
      // POST /data/<filename>
      post {
        path(Segment) { filename =>
          entity(as[Array[Byte]]) { bytes =>
          onComplete(dataManager ? StoreData(filename, bytes)) {
            case Success(value) => value match {
              case Stored(name) =>
                val map = Map[String, Any](ResultKey -> Map("filename" -> name))
                complete(StatusCodes.OK, map)
              case Error =>
                complete(StatusCodes.BadRequest, errMap(s"Failed to store data file '$filename'."))
            }
            case Failure(ex) =>
              complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
          }

          }
        }
      }
  }
