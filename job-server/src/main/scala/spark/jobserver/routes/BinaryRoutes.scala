package spark.jobserver.routes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import spark.jobserver.common.akka.web.JsonUtils._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshalling.ToResponseMarshallable._
import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCodes, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, _}
import akka.pattern.ask
import akka.util.Timeout
import org.joda.time.DateTime
import spark.jobserver.WebApiUtils.errMap
import spark.jobserver._
import spark.jobserver.io.BinaryType
import spray.json.DefaultJsonProtocol._

import scala.util.{Failure, Success}

/**
  * Routes for listing and uploading binaries
  *    GET /binaries              - lists all current binaries
  *    POST /binaries/<appName>   - upload a new binary file
  *    DELETE /binaries/<appName> - delete defined binary
  *
  * NB when POSTing new binaries, the content-type header must
  * be set to one of the types supported by the subclasses of the
  * `BinaryType` trait. e.g. "application/java-archive" or
  * application/python-archive" (may be expanded to support other types
  * in future.
  *
  */
trait BinaryRoutes extends AuthHelper {

  def binaryManager: ActorRef

  def contentType: Directive1[Option[ContentType]]

  def binaryRoutes(authenticator: AuthMethod)(implicit timeout: Timeout): Route = pathPrefix("binaries") {
    // user authentication
    authenticateOrRejectWithChallenge(authenticator) { _ =>
      // GET /binaries route returns a JSON map of the app name
      // and the type of and last upload time of a binary.
      get {
        onComplete(
          (binaryManager ? ListBinaries(None))
            .mapTo[collection.Map[String, (BinaryType, DateTime)]]
        ) {
          case Success(binTimeMap) =>
            val stringTimeMap = binTimeMap.map {
              case (app, (binType, dt)) =>
                (
                  app,
                  Map(
                    "binary-type" -> binType.name,
                    "upload-time" -> dt.toString()
                  )
                )
            }.toMap
            complete(stringTimeMap)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
        }

      } ~
        // POST /binaries/<appName>
        // The <appName> needs to be unique; uploading a jar with the same appName will replace it.
        // requires a recognised content-type header
        post {
          path(Segment) { appName =>
            entity(as[Array[Byte]]) { binBytes: Array[Byte] =>
              contentType {
                case Some(x) =>
                  BinaryType.fromMediaType(x.mediaType) match {
                    case Some(binaryType) =>
                      onComplete(
                        binaryManager ? StoreBinary(
                          appName,
                          binaryType,
                          binBytes
                        )
                      ) {
                        case Success(value) =>
                          value match {
                            case BinaryStored => complete(StatusCodes.OK)
                            case InvalidBinary =>
                              complete(
                                StatusCodes.BadRequest,
                                "Binary is not of the right format"
                              )
                            case BinaryStorageFailure(ex) =>
                              complete(
                                StatusCodes.InternalServerError,
                                errMap(ex, "Storage Failure")
                              )
                          }
                        case Failure(ex) =>
                          complete(
                            StatusCodes.InternalServerError,
                            errMap(ex, "ERROR")
                          )

                      }

                    case None => complete(415, s"Unsupported binary type $x")
                  }

                case None =>
                  complete(
                    415,
                    s"Content-Type header must be set to indicate binary type"
                  )
              }
            }
          }
        } ~
        // DELETE /binaries/<appName>
        delete {
          path(Segment) { appName =>
            onComplete(binaryManager ? DeleteBinary(appName)) {
              case Success(value) =>
                value match {
                  case BinaryDeleted =>
                    complete(StatusCodes.OK)
                  case NoSuchBinary =>
                    complete(
                      StatusCodes.NotFound,
                      errMap(s"can't find binary with name $appName")
                    )
                }
              case Failure(ex) =>
                complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
            }

          }
        }
    }
  }

}
