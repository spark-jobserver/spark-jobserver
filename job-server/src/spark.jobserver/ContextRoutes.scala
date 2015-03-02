package spark.jobserver

import akka.actor.ActorRef
import akka.pattern.ask

import com.typesafe.config.ConfigFactory

import com.wordnik.swagger.annotations._

import spray.http.MediaTypes
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._
import spray.routing.{HttpService, Route}

import scala.concurrent.ExecutionContext

@Api(value = "/contexts", description = "listing, adding, and stopping contexts")
trait ContextRoutes extends HttpService with CommonRouteBehaviour  {
  import CommonMessages._
  import ContextSupervisor._
  import scala.concurrent.duration._
  import ContextSupervisor._
  import collection.JavaConverters._
  import ooyala.common.akka.web.JsonUtils._

  implicit def ec: ExecutionContext =actorRefFactory.dispatcher

  val contextTimeout: Int

  val supervisor: ActorRef
  /**
   * Routes for listing, adding, and stopping contexts
   *     GET /contexts         - lists all current contexts
   *     POST /contexts/<contextName> - creates a new context
   *     DELETE /contexts/<contextName> - stops a context and all jobs running in it
   */
  def contextRoutes: Route = pathPrefix("contexts") {
    getRoute ~ postRoute ~ deleteRoute }

  @ApiOperation(httpMethod = "GET", response = classOf[String],
    value = "lists all current contexts")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "List of all current contexts")))
  def getRoute: Route = {
    import ContextSupervisor._
    import collection.JavaConverters._
    get {ctx =>
      (supervisor ? ListContexts).mapTo[Seq[String]]
      .map {contexts => ctx.complete (contexts)}
    }
  }

  @ApiOperation(httpMethod = "POST", response = classOf[String],
    value = "Creates a long-running context with contextName and options for context creation",
    notes = "All options are merged into the defaults in spark.context-settings")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "contextName", required = true, dataType = "String", paramType = "path",
      value = "Name for the context"),
    new ApiImplicitParam(name = "num-cpu-cores", required = false, dataType = "Int", paramType = "query",
      value = "Number of cores the context will use"),
    new ApiImplicitParam(name = "memory-per-node", required = false, dataType = "String",
      paramType = "query",
      value = "-Xmx style string (512m, 1g, etc) for max memory per node")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 400, message = "if context exists or could not be initialized")))
  def postRoute: Route = post {
        /**
         *  POST /contexts/<contextName>?<optional params> -
         *    Creates a long-running context with contextName and options for context creation
         *    All options are merged into the defaults in spark.context-settings
         *
         * @optional @param num-cpu-cores Int - Number of cores the context will use
         * @optional @param memory-per-node String - -Xmx style string (512m, 1g, etc) for max memory per node
         * @return the string "OK", or error if context exists or could not be initialized
         */
        path(Segment) { (contextName) =>
          // Enforce user context name to start with letters
          if (!contextName.head.isLetter) {
            complete(StatusCodes.BadRequest, errMap("context name must start with letters"))
          } else {
            parameterMap { (params) =>
              val config = ConfigFactory.parseMap(params.asJava)
              val future = (supervisor ? AddContext(contextName, config))(contextTimeout.seconds)
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case ContextInitialized   => ctx.complete(StatusCodes.OK)
                  case ContextAlreadyExists => badRequest(ctx, "context " + contextName + " exists")
                  case ContextInitError(e)  => ctx.complete(500, errMap(e, "CONTEXT INIT ERROR"))
                }
              }
            }
          }
        }
      }

  @ApiOperation(httpMethod = "DELETE", response = classOf[String],
    value = "Stop the context with the given name",
    notes = "Executors will be shut down and all cached RDDs and currently running jobs will be lost. " +
      " Use with care!")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "contextName", required = true, dataType = "File", paramType = "query",
      value = "Name of the context to delete")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 400, message = "if context exists or could not be initialized")))
  def deleteRoute: Route = delete {
        //  DELETE /contexts/<contextName>
        //  Stop the context with the given name.  Executors will be shut down and all cached RDDs
        //  and currently running jobs will be lost.  Use with care!
        path(Segment) { (contextName) =>
          val future = supervisor ? StopContext(contextName)
          respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
            future.map {
              case ContextStopped => ctx.complete(StatusCodes.OK)
              case NoSuchContext  => notFound(ctx, "context " + contextName + " not found")
            }
          }
        }
      }

}
