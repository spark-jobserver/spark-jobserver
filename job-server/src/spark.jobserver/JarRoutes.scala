package spark.jobserver

import com.wordnik.swagger.annotations._
import spray.http.{MediaTypes, StatusCodes}
import spray.routing.{HttpService, Route}

import akka.actor.{ ActorSystem, ActorRef }
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory, ConfigException, ConfigRenderOptions }
import java.util.NoSuchElementException
import ooyala.common.akka.web.{ WebService, CommonRoutes }
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.util.SparkJobUtils
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try
import spark.jobserver.io.JobInfo
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._

@Api(value = "/jars", description = "Jar Operations")
trait JarRoutes extends HttpService with CommonRouteBehaviour {
  import CommonMessages._
  import ContextSupervisor._
  import scala.concurrent.duration._
  import ContextSupervisor._
  import collection.JavaConverters._
  import ooyala.common.akka.web.JsonUtils._

  implicit def ec: ExecutionContext =actorRefFactory.dispatcher
  def jarManager: ActorRef
  /**
   * Routes for listing and uploading jars
   *    GET /jars              - lists all current jars
   *    POST /jars/<appName>   - upload a new jar file
   */
  def jarRoutes: Route = pathPrefix("jars") {
    getJarRoute ~ postJarRoute
  }

  @ApiOperation(httpMethod = "GET", response = classOf[String],
    value = "Returns a JSON map of the app name and the " +
    "last time a jar was uploaded")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "List of app names")))
  // GET /jars route returns a JSON map of the app name and the last time a jar was uploaded.
  def getJarRoute: Route = get {
    import ContextSupervisor._
    import collection.JavaConverters._
    ctx =>
    val future = (jarManager ? ListJars).mapTo[collection.Map[String, DateTime]]
    future.map { jarTimeMap =>
      val stringTimeMap = jarTimeMap.map { case (app, dt) => (app, dt.toString()) }.toMap
      ctx.complete(stringTimeMap)
    }.recover {
      case e: Exception => ctx.complete(500, errMap(e, "ERROR"))
    }
  }

  @ApiOperation(httpMethod = "POST", response = classOf[String], value = "Uploads a jar for appName")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "appName", required = true, dataType = "String", paramType = "path",
      value = "name of the app. It needs to be unique; uploading a jar with the same appName will " +
        "replace it."),
    new ApiImplicitParam(name = "body", required = true, dataType = "File", paramType = "body",
      value = "an assembly jar containing your spark code.")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Jar in wrong format")))
  def postJarRoute: Route = post {
      path(Segment) { appName =>
        entity(as[Array[Byte]]) { jarBytes =>
          val future = jarManager ? StoreJar(appName, jarBytes)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            future.map {
              case JarStored  => ctx.complete(StatusCodes.OK)
              case InvalidJar => badRequest(ctx, "Jar is not of the right format")
            }.recover {
              case e: Exception => ctx.complete(500, errMap(e, "ERROR"))
            }
          }
        }
      }
    }

}
