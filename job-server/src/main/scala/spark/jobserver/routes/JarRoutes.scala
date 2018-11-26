package spark.jobserver.routes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import spark.jobserver.common.akka.web.JsonUtils._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshalling.ToResponseMarshallable._

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import org.joda.time.DateTime
import spark.jobserver.WebApiUtils.{errMap, successMap}
import spark.jobserver._
import spark.jobserver.io.BinaryType
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * Routes for listing and uploading jars
  *    GET /jars              - lists all current jars
  *    POST /jars/<appName>   - upload a new jar file
  *
  * NB these routes are kept for legacy purposes but are deprecated in favour
  *  of the /binaries routes.
  */
trait JarRoutes extends AuthHelper {

  def binaryManager: ActorRef

  def jarRoutes(authenticator: AuthMethod)
               (implicit timeout: Timeout, ec: ExecutionContext): Route = pathPrefix("jars") {
    // user authentication
    authenticateOrRejectWithChallenge(authenticator) { _ =>
      // GET /jars route returns a JSON map of the app name and the last time a jar was uploaded.
      get {
        onComplete(
          (binaryManager ? ListBinaries(Some(BinaryType.Jar)))
            .mapTo[collection.Map[String, (BinaryType, DateTime)]]
            .map(_.mapValues(_._2))
        ) {
          case Success(jarTimeMap) =>
            val stringTimeMap = jarTimeMap.map {
              case (app, dt) => (app, dt.toString())
            }.toMap
            complete(stringTimeMap)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
        }
      } ~
        // POST /jars/<appName>
        // The <appName> needs to be unique; uploading a jar with the same appName will replace it.
        post {
          path(Segment) { appName =>
            entity(as[Array[Byte]]) { jarBytes =>
              onComplete(
                binaryManager ? StoreBinary(appName, BinaryType.Jar, jarBytes)
              ) {
                case Success(value) =>
                  value match {
                    case BinaryStored =>
                      complete(StatusCodes.OK, successMap("Jar uploaded"))
                    case InvalidBinary =>
                      complete(
                        StatusCodes.BadRequest,
                        errMap("Jar is not of the right format")
                      )
                    case BinaryStorageFailure(ex) =>
                      complete(
                        StatusCodes.InternalServerError,
                        errMap(ex, "Storage Failure")
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
}
