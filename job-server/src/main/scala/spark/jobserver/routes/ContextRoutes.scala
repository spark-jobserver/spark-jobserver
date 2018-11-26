package spark.jobserver.routes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import spark.jobserver.common.akka.web.JsonUtils._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshalling.ToResponseMarshallable._
import akka.actor.ActorRef
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.Logger
import spark.jobserver.ContextSupervisor
import spark.jobserver.WebApiUtils.{errMap, getContextReport, successMap}
import spark.jobserver.auth.AuthInfo
import spark.jobserver.util.SparkJobUtils
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Routes for listing, adding, and stopping contexts
  *     GET /contexts         - lists all current contexts
  *     GET /contexts/<contextName> - returns some info about the context (such as spark UI url)
  *     POST /contexts/<contextName> - creates a new context
  *     DELETE /contexts/<contextName> - stops a context and all jobs running in it
  */
trait ContextRoutes extends AuthHelper with ConfigHelper {

  def logger: Logger

  def supervisor: ActorRef

  def contextRoutes(authenticator: AuthMethod, config: Config,
                    contextTimeout: Int, contextDeletionTimeout: Int)
                   (implicit timeout: Timeout, ec: ExecutionContext): Route = pathPrefix("contexts") {
    import ContextSupervisor._

    // user authentication
    authenticateOrRejectWithChallenge(authenticator) { user =>
      (get & path(Segment)) { contextName =>
        onComplete(supervisor ? GetSparkContexData(contextName)) {
          case Success(value) =>
            value match {
              case SparkContexData(context, appId, url) =>
                val stcode = 200
                val contextMap = getContextReport(context, appId, url)
                logger.info("StatusCode: " + stcode + ", " + contextMap)
                complete(stcode, contextMap)
              case NoSuchContext =>
                complete(
                  StatusCodes.BadRequest,
                  errMap(s"can't find context with name $contextName")
                )
              case UnexpectedError =>
                complete(
                  StatusCodes.InternalServerError,
                  errMap("UNEXPECTED ERROR OCCURRED")
                )
            }
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
        }

      } ~
        get {
          logger.info("GET /contexts")
          onComplete(supervisor ? ListContexts) {
            case Success(value) =>
              value match {
                case UnexpectedError =>
                  complete(
                    StatusCodes.InternalServerError,
                    errMap("UNEXPECTED ERROR OCCURRED")
                  )
                case contexts =>
                  val getContexts = SparkJobUtils.removeProxyUserPrefix(
                    user.toString,
                    contexts.asInstanceOf[Seq[String]],
                    config.getBoolean("shiro.authentication") &&
                      config.getBoolean("shiro.use-as-proxy-user")
                  )
                  complete(getContexts)
              }
            case Failure(ex) =>
              complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
          }

        } ~
        post {

          /**
            *  POST /contexts/<contextName>?<optional params> -
            *    Creates a long-running context with contextName and options for context creation
            *    All options are merged into the defaults in spark.context-settings
            *
            * @optional @entity The POST entity should be a Typesafe Config format file with a
            *            "spark.context-settings" block containing spark configs for the context.
            * @optional @param num-cpu-cores Int - Number of cores the context will use
            * @optional @param memory-per-node String - -Xmx style string (512m, 1g, etc)
            * for max memory per node
            * @return the string "OK", or error if context exists or could not be initialized
            */
          entity(as[String]) { configString =>
            withRequestTimeout(contextTimeout.seconds) {
              path(Segment) { cn =>
                val contextName = cn
                // Enforce user context name to start with letters
                if (!contextName.head.isLetter) {
                  complete(
                    StatusCodes.BadRequest,
                    errMap("context name must start with letters")
                  )
                } else {
                  parameterMap { params =>
                    // parse the config from the body, using url params as fallback values
                    val baseConfig = ConfigFactory.parseString(configString)
                    val contextConfig = getContextConfig(baseConfig, params)
                    val (cName, config) = determineProxyUser(
                      contextConfig,
                      AuthInfo(user),
                      contextName
                    )

                    onComplete(supervisor ? AddContext(cName, config)) {
                      case Success(value) =>
                        value match {
                          case ContextInitialized =>
                            complete(
                              StatusCodes.OK,
                              successMap("Context initialized")
                            )
                          case ContextAlreadyExists =>
                            complete(
                              StatusCodes.BadRequest,
                              errMap(s"context $contextName exists")
                            )
                          case ContextInitError(e) =>
                            complete(
                              StatusCodes.InternalServerError,
                              errMap(e, "CONTEXT INIT ERROR")
                            );
                          case UnexpectedError =>
                            complete(
                              StatusCodes.InternalServerError,
                              errMap("UNEXPECTED ERROR OCCURRED")
                            )
                        }
                      case Failure(ex) =>
                        complete(
                          StatusCodes.InternalServerError,
                          errMap(ex, "ERROR")
                        )
                    }
                  }
                }
              }
            }
          }
        } ~
        (delete & path(Segment)) { contextName =>
          //  DELETE /contexts/<contextName>
          //  Stop the context with the given name.  Executors will be shut down and all cached RDDs
          //  and currently running jobs will be lost.  Use with care!
          //  If force=true flag is provided, will kill the job forcefully
          parameters('force.as[Boolean] ?) { forceOpt => {
            val force = forceOpt.getOrElse(false)
            logger.info(s"DELETE /contexts/$contextName")
            val (cName, _) =
              determineProxyUser(config, AuthInfo(user), contextName)
            withRequestTimeout(contextDeletionTimeout.seconds + 1.seconds) {
              extractUri { uri =>
                onComplete(supervisor ? StopContext(cName, force)) {
                  case Success(value) =>
                    value match {
                      case ContextStopped =>
                        complete(StatusCodes.OK, successMap("Context stopped"))
                      case ContextStopInProgress =>
                        val response = HttpResponse(
                          status = StatusCodes.Accepted,
                          headers = List(Location(uri))
                        )
                        complete(response)
                      case NoSuchContext =>
                        complete(
                          StatusCodes.NotFound,
                          errMap(s"context $contextName not found")
                        )
                      case ContextStopError(e) =>
                        complete(
                          StatusCodes.InternalServerError,
                          errMap(e, "CONTEXT DELETE ERROR")
                        )
                      case UnexpectedError =>
                        complete(
                          StatusCodes.InternalServerError,
                          errMap("UNEXPECTED ERROR OCCURRED")
                        )
                    }
                  case Failure(ex) =>
                    complete(
                      StatusCodes.InternalServerError,
                      errMap(ex, "ERROR")
                    )
                }
              }
            }
          }
          }
        } ~
        put {
          parameters("reset", 'sync.as[Boolean] ?) { (reset, sync) =>
            reset match {
              case "reboot" =>
                import java.util.concurrent.TimeUnit

                import ContextSupervisor._
                val lookupTimeout = Try(
                  config
                    .getDuration("spark.jobserver.context-lookup-timeout", TimeUnit.MILLISECONDS)
                    .toInt / 1000
                ).getOrElse(1) + contextTimeout
                withRequestTimeout(lookupTimeout.seconds) {
                  onComplete((supervisor ? ListContexts).mapTo[Seq[String]]) {
                    case Success(contexts) =>
                      if (sync.isDefined && !sync.get) {
                        contexts.foreach(c => supervisor ! StopContext(c))
                        complete(
                          StatusCodes.OK,
                          successMap("Context reset requested")
                        )
                      } else {
                        onComplete(
                          Future.sequence(
                            contexts.map(c => supervisor ? StopContext(c))
                          )
                        ) {
                          case Success(_) =>
                            //                          Thread.sleep(1000) // we apparently need some sleeping
                            // in here, so spark can catch up
                            onComplete(supervisor ? AddContextsFromConfig) {
                              case Success(_) =>
                                complete(
                                  StatusCodes.OK,
                                  successMap("Context reset")
                                )
                              case Failure(ex) =>
                                complete( "ERROR")
                            }
                          case Failure(ex) =>
                            complete(
                              StatusCodes.InternalServerError,
                              errMap(ex, "ERROR")
                            )
                        }
                      }
                    case Failure(ex) =>
                      complete(
                        StatusCodes.InternalServerError,
                        errMap(ex, "ERROR")
                      )
                  }
                }
              case _ =>
                complete("ERROR")
            }
          }
        }
    }
  }

}
