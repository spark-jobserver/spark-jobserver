package spark.jobserver

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Location, `Content-Type`}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.LoggingMagnet
import akka.http.scaladsl.server.{Directive0, RequestContext, Route, RouteResult}
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.config._
import org.slf4j.{Logger, LoggerFactory}
import spark.jobserver.auth.Permissions._
import spark.jobserver.auth.SJSAccessControl._
import spark.jobserver.auth._
import spark.jobserver.common.akka.web.JsonUtils.AnyJsonFormat
import spark.jobserver.common.akka.web.{CommonRoutes, WebService}
import spark.jobserver.io.JobDAOActor._
import spark.jobserver.io._
import spark.jobserver.routes.DataRoutes
import spark.jobserver.util.ResultMarshalling._
import spark.jobserver.util._

import java.net.{MalformedURLException, URLDecoder}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatterBuilder
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object WebApi extends SprayJsonSupport {
  import spray.json.DefaultJsonProtocol._

  private val df = new DateTimeFormatterBuilder().appendInstant(3).toFormatter()
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def completeWithErrorStatus(ctx: RequestContext, errMsg: String, stcode: StatusCode,
                              exception: Option[Throwable] = None): Future[RouteResult] = {
    val message: Map[String, Any] = exception match {
      case None =>
        logger.info("StatusCode: " + stcode + ", ErrorMessage: " + errMsg)
        Map(StatusKey -> JobStatus.Error, ResultKey -> errMsg)
      case _ =>
        val ex = exception.get
        logger.info("StatusCode: " + stcode.intValue + ", ErrorMessage: " + errMsg + ", StackTrace: "
          + ErrorData.getStackTrace(ex))
        Map(StatusKey -> errMsg, ResultKey -> formatException(ex))
    }
    ctx.complete(stcode.intValue, message)
  }

  def completeWithException(ctx: RequestContext, errMsg: String, stcode: StatusCode,
                            e: Throwable): Future[RouteResult] =
    completeWithErrorStatus(ctx, errMsg, stcode, Some(e))

  def completeWithSuccess(ctx: RequestContext, statusCode: StatusCode,
                          msg: String = ""): Future[RouteResult] = {
    val resultMap = msg match {
      case "" => Map(StatusKey -> "SUCCESS")
      case _ => Map(StatusKey -> "SUCCESS", ResultKey -> msg)
    }
    ctx.complete(statusCode, resultMap)
  }

  def getContextReport(context: Any, appId: Option[String], url: Option[String]): Map[String, String] = {
    import scala.collection.mutable
    val map = mutable.Map.empty[String, String]
    context match {
      case contextInfo: ContextInfo =>
        map("id") = contextInfo.id
        map("name") = contextInfo.name
        map("startTime") = df.format(contextInfo.startTime)
        map("endTime") = contextInfo.endTime.map(df.format).getOrElse("Empty")
        map("state") = contextInfo.state
      case name: String =>
        map("name") = name
      case _ =>
    }
    (appId, url) match {
      case (Some(id), Some(u)) =>
        map("applicationId") = id
        map("url") = u
      case (Some(id), None) =>
        map("applicationId") = id
      case _ =>
    }
    map.toMap
  }

  def respondWithMediaType(mediaType: MediaType.WithFixedCharset): Directive0 = {
    mapResponse(res =>
      res.withEntity(res.entity.withContentType(ContentType(mediaType)))
    )
  }
}

class WebApi(system: ActorSystem,
             config: Config,
             port: Int,
             binaryManager: ActorRef,
             dataManager: ActorRef,
             supervisor: ActorRef,
             daoActor: ActorRef,
             healthCheckInst: HealthCheck)
    extends MeteredHttpService with CommonRoutes with DataRoutes
                        with ChunkEncodedStreamingSupport {
  import CommonMessages._
  import ContextSupervisor._
  import WebApi._

  import scala.concurrent.duration._

  implicit val actorRefFactory: ActorSystem = system
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val ShortTimeout =
    Timeout(config.getDuration("spark.jobserver.short-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  val DefaultSyncTimeout = Timeout(10 seconds)
  val DefaultBinaryDeletionTimeout = Timeout(10.seconds)
  val DefaultJobLimit = 50
  val ResultChunkSize = Option("spark.jobserver.result-chunk-size").filter(config.hasPath)
      .fold(100 * 1024)(config.getBytes(_).toInt)

  val contextTimeout = SparkJobUtils.getContextCreationTimeout(config)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  val bindAddress = config.getString("spark.jobserver.bind-address")

  private val maxSprayLogMessageLength = 400

  // Response code counters
  private val internalServerErrorCounter = counter("internal-server-error-status-code")
  // Timer metrics
  private val binGet = timer("binary-get-duration", TimeUnit.MILLISECONDS)
  private val binGetSingle = timer("binary-get-single-duration", TimeUnit.MILLISECONDS)
  private val binPost = timer("binary-post-duration", TimeUnit.MILLISECONDS)
  private val binDelete = timer("binary-delete-duration", TimeUnit.MILLISECONDS)
  private val contextGet = timer("context-get-duration", TimeUnit.MILLISECONDS)
  private val contextGetSingle = timer("context-get-single-duration", TimeUnit.MILLISECONDS)
  private val contextPost = timer("context-post-duration", TimeUnit.MILLISECONDS)
  private val contextDelete = timer("context-delete-duration", TimeUnit.MILLISECONDS)
  private val contextPut = timer("context-put-duration", TimeUnit.MILLISECONDS)
  private val jobGet = timer("job-get-duration", TimeUnit.MILLISECONDS)
  private val jobGetSingle = timer("job-get-single-duration", TimeUnit.MILLISECONDS)
  private val jobPost = timer("job-post-duration", TimeUnit.MILLISECONDS)
  private val jobDelete = timer("job-delete-duration", TimeUnit.MILLISECONDS)
  private val configGet = timer("config-get-duration", TimeUnit.MILLISECONDS)

  val accessLogger: LoggingMagnet[HttpRequest => RouteResult => Unit] = LoggingMagnet {
    _ =>
      request: HttpRequest =>
        logger.info(s"[${request.method}] ${request.uri}")
        _ match {
          case RouteResult.Complete(res) =>
            val resMessage = res.entity.toString.
              replace('\n', ' ').replaceAll(" +", " ")
            val statusCode = res.status.intValue
            if (statusCode == StatusCodes.InternalServerError.intValue) {
              internalServerErrorCounter.inc()
            }
            logger.info(s"Response: $statusCode " +
              s"${resMessage.substring(0, Math.min(resMessage.length(), maxSprayLogMessageLength))}(...) " +
              s"to request: ${request.uri}")
          case _ => logger.info("Unknown response")
        }
  }

  val myRoutes = logRequestResult(accessLogger) {
    cors() {
      extractRequestContext { ctx =>
        withRequestTimeoutResponse(_ => timeoutRoute(ctx)) {
          overrideMethodWithParameter("_method") {
            binaryRoutes ~ contextRoutes ~ jobRoutes ~
              dataRoutes ~ healthzRoutes ~ otherRoutes
          }
        }
      }
    }
  }

  lazy val authenticator: Challenge = {
    val authConfig = Try(config.getConfig("access-control"))
      .toOption.getOrElse(ConfigFactory.empty())
    val providerClass = Try(authConfig.getString("provider"))
      .toOption.getOrElse("spark.jobserver.auth.AllowAllAccessControl")
    logger.info(f"Using $providerClass authentication provider.")

    val providerInst = Class.forName(providerClass)
      .getConstructors()(0)
      .newInstance(authConfig, ec, system)
      .asInstanceOf[SJSAccessControl]
    providerInst.challenge()
  }

  def start() {
    /**
     * activates ssl or tsl encryption between client and SJS if so requested
     * in config
     */
    val sslConfig = config.getConfig("akka.http.server")
    val sslContext: Option[SSLContext] =
      if (sslConfig.hasPath("ssl-encryption") && sslConfig.getBoolean("ssl-encryption")) {
        Some(SSLContextFactory.createContext(sslConfig))
      } else {
        None
      }

    logger.info("Starting browser web service...")
    WebService.start(myRoutes ~ commonRoutes, system, bindAddress, port, sslContext)
  }

  /**
    * Routes for listing and uploading binaries
    *    GET /binaries              - lists all current binaries
    *    GET /binaries/<appName>    - returns binary information for binary with this appName
    *    POST /binaries/<appName>   - upload a new binary file
    *    DELETE /binaries/<appName> - delete defined binary
    *
    * NB when POSTing new binaries, the content-type header must
    * be set to one of the types supported by the subclasses of the
    * `BinaryType` trait. e.g. "application/java-archive", "application/python-egg" or
    * "application/python-wheel" (may be expanded to support other types
    * in future).
    *
    */
  def binaryRoutes: Route = pathPrefix("binaries") {
    // user authentication
    Route.seal(customAuthenticateBasicAsync(authenticator) { authInfo =>
      // GET /binaries/<appName>
      (get & path(Segment) & authorize(authInfo.hasPermission(BINARIES_READ))) { appName =>
        import spray.json.DefaultJsonProtocol._

        val timer = binGetSingle.time()
        respondWithMediaType(MediaTypes.`application/json`) { ctx =>
          val future = (binaryManager ? GetBinary(appName)).mapTo[LastBinaryInfo]
          future.flatMap{
            case LastBinaryInfo(Some(bin)) =>
              val res = Map("app-name" -> bin.appName,
                  "binary-type" -> bin.binaryType.name,
                  "upload-time" -> df.format(bin.uploadTime))
              ctx.complete(StatusCodes.OK, res)
            case LastBinaryInfo(None) =>
              completeWithErrorStatus(ctx, s"Can't find binary with name $appName", StatusCodes.NotFound)
            case _ =>
              completeWithErrorStatus(ctx, "UNEXPECTED ERROR OCCURRED", StatusCodes.InternalServerError)
          }.recoverWith {
            case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
          }.andThen {
            case _ => timer.stop()
          }
        }
      } ~
      // GET /binaries route returns a JSON map of the app name
      // and the type of and last upload time of a binary.
      (get & authorize(authInfo.hasPermission(BINARIES_READ))) { ctx =>
        import spray.json.DefaultJsonProtocol._

        val timer = binGet.time()
        val future = (binaryManager ? ListBinaries(None)).
          mapTo[collection.Map[String, (BinaryType, ZonedDateTime)]]
        future.flatMap { binTimeMap =>
          val stringTimeMap = binTimeMap.map {
            case (app, (binType, dt)) =>
              (app, Map("binary-type" -> binType.name, "upload-time" -> df.format(dt)))
          }.toMap
          ctx.complete(stringTimeMap)
        }.recoverWith {
          case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
        }.andThen {
          case _ => timer.stop()
        }
      } ~
      // POST /binaries/<appName>
      // The <appName> needs to be unique; uploading a jar with the same appName will replace it.
      // requires a recognised content-type header
      (post & authorize(authInfo.hasPermission(BINARIES_UPLOAD))) {
        path(Segment) { appName =>
          val timer = binPost.time()
          entity(as[Array[Byte]]) { binBytes =>
            extractRequest { request =>
                respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                  // In test cases entity.contentType is 'application/json' even though the content-type
                  // header is set. For tests header has to be explicitly extracted
                  val x: ContentType = request.headers.map({
                    case `Content-Type`(ct) => Some(ct)
                    case _ => None
                  }).find(_.isDefined)
                    .flatten
                    .getOrElse(request.entity.contentType)
                  BinaryType.fromMediaType(x.mediaType) match {
                    case Some(binaryType) =>
                      val future = binaryManager ? StoreBinary(appName, binaryType, binBytes)

                      future.flatMap {
                        case BinaryStored =>
                          completeWithSuccess(ctx, StatusCodes.Created)
                        case InvalidBinary =>
                          completeWithErrorStatus(
                            ctx, "Binary is not of the right format", StatusCodes.BadRequest)
                        case BinaryStorageFailure(ex) => completeWithException(
                          ctx, "Storage Failure", StatusCodes.InternalServerError, ex)
                        case _ => completeWithErrorStatus(
                          ctx, "Unhandled state", StatusCodes.InternalServerError)
                      }.recoverWith {
                        case e: Exception => completeWithException(
                          ctx, "ERROR", StatusCodes.InternalServerError, e)
                      }.andThen {
                        case _ => timer.stop()
                      }
                    case None =>
                      timer.stop()
                      completeWithErrorStatus(
                        ctx, s"Unsupported binary type $x", StatusCodes.UnsupportedMediaType)
                  }
                }
            }
          }
        }
      } ~
      // DELETE /binaries/<appName>
      (delete & authorize(authInfo.hasPermission(BINARIES_DELETE))) {
        path(Segment) { appName =>
          val timer = binDelete.time()
          val future = (binaryManager ? DeleteBinary(appName))(DefaultBinaryDeletionTimeout)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            future.flatMap {
              case BinaryDeleted =>
                completeWithSuccess(ctx, StatusCodes.OK)
              case BinaryInUse(jobs) =>
                completeWithErrorStatus(ctx,
                              s"Binary is in use by job(s): ${jobs.mkString(", ")}",
                              StatusCodes.Forbidden)
              case NoSuchBinary(name) =>
                completeWithErrorStatus(ctx, s"can't find binary with name $name", StatusCodes.NotFound)
              case BinaryDeletionFailure(ex) =>
                completeWithErrorStatus(ctx,
                              s"Failed to delete binary due to internal error. Check logs.",
                              StatusCodes.InternalServerError)
            }.recoverWith {
              case e: Exception => completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
            }.andThen {
              case _ => timer.stop()
            }
          }
        }
      }
    })
  }

  /**
   * Routes for listing, deletion of and storing data files
   *    GET /data                     - lists all currently stored files
   *    DELETE /data/<filename>       - deletes given file, no-op if file does not exist
   *    POST /data/<filename-prefix>  - upload a new data file, using the given prefix,
   *                                      a time stamp is appended to ensure uniqueness
   * @author TimMaltGermany
   */
  def dataRoutes: Route = pathPrefix("data") {
    // user authentication
    Route.seal(customAuthenticateBasicAsync(authenticator) { authInfo =>
      dataRoutes(dataManager, authInfo)
    })
  }

  /**
   * Routes for listing, adding, and stopping contexts
   *     GET /contexts         - lists all current contexts
   *     GET /contexts/<contextName> - returns some info about the context (such as spark UI url)
   *     POST /contexts/<contextName> - creates a new context
   *     DELETE /contexts/<contextName> - stops a context and all jobs running in it
   */
  def contextRoutes: Route = pathPrefix("contexts") {
    import ContextSupervisor._

    // user authentication
    Route.seal(customAuthenticateBasicAsync(authenticator) { authInfo =>
      (get & path(Segment) & authorize(authInfo.hasPermission(CONTEXTS_READ))) { contextName =>
        import spray.json.DefaultJsonProtocol._
        val timer = contextGetSingle.time()
        respondWithMediaType(MediaTypes.`application/json`) { ctx =>
          val future = (supervisor ? GetSparkContexData(contextName))(15.seconds)
          future.flatMap {
            case SparkContexData(context, appId, url) =>
              val contextMap = getContextReport(context, appId, url)
              ctx.complete(StatusCodes.OK, contextMap)
            case NoSuchContext =>
              completeWithErrorStatus(ctx, s"can't find context with name $contextName", StatusCodes.NotFound)
            case UnexpectedError => completeWithErrorStatus(
              ctx, "UNEXPECTED ERROR OCCURRED", StatusCodes.InternalServerError)
            case _ => completeWithErrorStatus(
              ctx, "Unhandled error", StatusCodes.InternalServerError)
          }.recoverWith {
            case e: Exception =>
              completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
          }.andThen{
            case _ => timer.stop()
          }
        }
      } ~
      (get & authorize(authInfo.hasPermission(CONTEXTS_READ))) { ctx =>
        import spray.json.DefaultJsonProtocol._

        val timer = contextGet.time()
        val future = supervisor ? ListContexts
        future.flatMap {
          case UnexpectedError => completeWithErrorStatus(
            ctx, "UNEXPECTED ERROR OCCURRED", StatusCodes.InternalServerError)
          case contexts =>
            val getContexts = SparkJobUtils.removeProxyUserPrefix(
              authInfo.toString, contexts.asInstanceOf[Seq[String]],
              config.hasPath("access-control.shiro") &&
                config.getBoolean("access-control.shiro.use-as-proxy-user"))
            ctx.complete(getContexts)
        }.recoverWith {
          case e: Exception =>
            completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
        }.andThen {
          case _ => timer.stop()
        }
      } ~
      (post & authorize(authInfo.hasPermission(CONTEXTS_START))) {
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
          path(Segment) { contextName =>
            val timer = contextPost.time()
            // Enforce user context name to start with letters
            if (!contextName.head.isLetter) {
              timer.stop()
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                completeWithErrorStatus(ctx, "context name must start with letters", StatusCodes.BadRequest)
              }
            } else {
              parameterMap { (params) =>
                // parse the config from the body, using url params as fallback values
                val baseConfig = ConfigFactory.parseString(configString)
                val contextConfig = getContextConfig(baseConfig, params)
                val (cName, config) = determineProxyUser(contextConfig, authInfo, contextName)
                val future = (supervisor ? AddContext(cName, config))(contextTimeout.seconds)
                respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                  future.flatMap {
                    case ContextInitialized =>
                      completeWithSuccess(ctx, StatusCodes.OK, "Context initialized")
                    case ContextAlreadyExists => completeWithErrorStatus(
                      ctx, "context " + contextName + " exists", StatusCodes.BadRequest)
                    case ContextInitError(e) => e match {
                      case _: MalformedURLException => completeWithException(
                        ctx, "CONTEXT INIT ERROR: Malformed URL", StatusCodes.BadRequest, e)
                      case _ => completeWithException(
                        ctx, "CONTEXT INIT ERROR", StatusCodes.InternalServerError, e)
                    }
                    case UnexpectedError => completeWithErrorStatus(
                      ctx, "UNEXPECTED ERROR OCCURRED", StatusCodes.InternalServerError)
                  }.recoverWith {
                    case e: Exception =>
                      completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
                  }.andThen {
                    case _ => timer.stop()
                  }
                }
              }
            }
          }
        }
      } ~
        (delete & path(Segment) & authorize(authInfo.hasPermission(CONTEXTS_DELETE))) { contextName =>
          //  DELETE /contexts/<contextName>
          //  Stop the context with the given name.  Executors will be shut down and all cached RDDs
          //  and currently running jobs will be lost.  Use with care!
          //  If force=true flag is provided, will kill the job forcefully
          parameters('force.as[Boolean] ?) {
            forceOpt => {
              val force = forceOpt.getOrElse(false)
              val timer = contextDelete.time()
              val (cName, _) = determineProxyUser(config, authInfo, contextName)
              val future = (supervisor ?
                StopContext(cName, force))(contextDeletionTimeout.seconds + 1.seconds)
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.flatMap {
                  case ContextStopped =>
                    completeWithSuccess(ctx, StatusCodes.OK, "Context stopped")
                  case ContextStopInProgress =>
                    val response = HttpResponse(
                      status = StatusCodes.Accepted, headers = List(Location(ctx.request.uri)))
                    ctx.complete(response)
                  case NoSuchContext => completeWithErrorStatus(
                    ctx, "context " + contextName + " not found", StatusCodes.NotFound)
                  case ContextStopError(e) => completeWithException(
                    ctx, "CONTEXT DELETE ERROR", StatusCodes.InternalServerError, e)
                  case UnexpectedError => completeWithErrorStatus(
                    ctx, "UNEXPECTED ERROR OCCURRED", StatusCodes.InternalServerError)
                }.recoverWith {
                  case e: Exception =>
                    completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
                }.andThen {
                  case _ => timer.stop()
                }
              }
            }
          }
        } ~
        put {
          parameters("reset", 'sync.as[Boolean] ?) { (reset, sync) =>
            authorize(authInfo.hasPermission(CONTEXTS_RESET)) {
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                val timer = contextPut.time()
                val ret = reset match {
                  case "reboot" =>
                    import ContextSupervisor._
                    import java.util.concurrent.TimeUnit

                    val future = (supervisor ? ListContexts).mapTo[Seq[String]]
                    val lookupTimeout = Try(config.getDuration("spark.jobserver.context-lookup-timeout",
                      TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(1)
                    val lookupTimeoutFut = akka.pattern.after(lookupTimeout.seconds, system.scheduler)(
                      Future.failed(new concurrent.TimeoutException())
                    )

                    Future.firstCompletedOf(Seq(future, lookupTimeoutFut))
                      .flatMap(contexts => {
                        if (sync.isDefined && !sync.get) {
                          contexts.foreach(c => supervisor ! StopContext(c))
                          completeWithSuccess(ctx, StatusCodes.OK, "Context reset requested")
                        } else {
                          val contextTimeoutFut = akka.pattern.after(contextTimeout.seconds,
                            system.scheduler)(Future.failed(new concurrent.TimeoutException()))
                          val stopFutures = contexts.map(c => supervisor ? StopContext(c))
                          Future.firstCompletedOf(Seq(Future.sequence(stopFutures), contextTimeoutFut))
                            .flatMap(_ => {
                              // we apparently need some sleeping in here, so spark can catch up
                              Thread.sleep(1000)
                              // Supervisor sends no answer. Do not check for failure
                              supervisor ! AddContextsFromConfig
                              completeWithSuccess(ctx, StatusCodes.OK, "Context reset")
                            })
                        }
                      })
                      .recoverWith {
                        case ex: Throwable =>
                          completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, ex)
                      }
                  case _ =>
                    completeWithErrorStatus(ctx, "ERROR", StatusCodes.InternalServerError)
                }
                timer.stop()
                ret
              }
            }
          }
        }
    })
  }

  /**
   * Routes for getting health status of job server
   *    GET /healthz              - return OK or error message
   */
  def healthzRoutes: Route = pathPrefix("healthz") {
    get { ctx =>
      try {
        if (healthCheckInst != null && healthCheckInst.isHealthy()) {
          completeWithSuccess(ctx, StatusCodes.OK)
        } else {
          completeWithErrorStatus(ctx, "Required actors not alive", StatusCodes.InternalServerError)
        }
      }
      catch {
        case ex: Exception => {
          logger.error("Exception in healthz", ex)
          completeWithErrorStatus(
            ctx, "Exception while invoking health check", StatusCodes.InternalServerError)
        }
      }
    }
  }

  def otherRoutes: Route = get {
    implicit val ar = actorRefFactory
    //no authentication required

    path("") {
      // Main index.html page
      getFromResource("html/index.html")
    } ~ pathPrefix("html") {
      // Static files needed by index.html
      getFromResourceDirectory("html")
    }
  }

  val errorEvents: Set[Class[_]] = Set(classOf[JobErroredOut], classOf[JobValidationFailed])
  val asyncEvents = Set(classOf[JobStarted]) ++ errorEvents
  val syncEvents = Set(classOf[JobResult]) ++ errorEvents

  /**
   * Main routes for starting a job, listing existing jobs, getting job results
   */
  def jobRoutes: Route = pathPrefix("jobs") {
    import JobManagerActor._

    // user authentication
    Route.seal(customAuthenticateBasicAsync(authenticator) { authInfo =>
      /**
       * GET /jobs/<jobId>/config --
       * returns the configuration used to launch this job or an error if not found.
       *
       * @required @param jobId
       */
      (get & path(Segment / "config") & authorize(authInfo.hasPermission(JOBS_READ))) { jobId =>
        val timer = configGet.time()
        val renderOptions = ConfigRenderOptions.defaults().setComments(false).setOriginComments(false)

        val future = (daoActor ? GetJobConfig(jobId)).mapTo[JobConfig]
        respondWithMediaType(MediaTypes.`application/json`) { ctx =>
          future.flatMap {
            case JobConfig(None) =>
              completeWithErrorStatus(ctx, "No such job ID " + jobId, StatusCodes.NotFound)
            case JobConfig(Some(cnf)) =>
              ctx.complete(cnf.root().render(renderOptions))
          }.recoverWith {
            case e: Exception =>
              completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
          }.andThen {
            case _ => timer.stop()
          }
        }
      } ~
        // GET /jobs/<jobId>
        // Returns job information in JSON.
        // If the job isn't finished yet, then {"status": "RUNNING" | "ERROR"} is returned.
        // Returned JSON contains result attribute if status is "FINISHED"
        (get & path(Segment) & authorize(authInfo.hasPermission(JOBS_READ))) { jobId =>
          import spray.json.DefaultJsonProtocol._
          val timer = jobGetSingle.time()
          val getJobInfoFuture = (daoActor ? GetJobInfo(jobId)).mapTo[Option[JobInfo]]
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            getJobInfoFuture.flatMap {
              case None =>
                completeWithErrorStatus(ctx, "No such job ID " + jobId, StatusCodes.NotFound)
              case Some(info) =>
                val jobReport = getJobReport(info)
                if (info.state != JobStatus.Finished) {
                  logger.info(jobReport.toString())
                  ctx.complete(jobReport)
                } else {
                  val resultFuture = for {
                    resultActor <- (supervisor ?
                      ContextSupervisor.GetResultActor(info.contextName)).mapTo[ActorRef]
                    result <- resultActor ? GetJobResult(jobId)
                  } yield result
                  resultFuture.flatMap {
                    case JobResult(_, result) =>
                      result match {
                        case s: Stream[_] =>
                          sendStreamingResponse(ctx, ResultChunkSize,
                            resultToByteIterator(jobReport, s.toIterator))
                        case _ =>
                          logger.info(jobReport.toString() + ", " + result)
                          ctx.complete(jobReport ++ resultToTable(result))
                      }
                    case _ =>
                      logger.info(jobReport.toString())
                      ctx.complete(jobReport)
                  }.recoverWith {
                    case e: Exception =>
                      completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
                  }
                }
            }.recoverWith {
              case e: Exception =>
                completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
            }.andThen {
              case _ => timer.stop()
            }
          }
        } ~
        //  DELETE /jobs/<jobId>
        //  Stop the current job. All other jobs submitted with this spark context
        //  will continue to run
        (delete & path(Segment) & authorize(authInfo.hasPermission(JOBS_DELETE))) { jobId =>
          import spray.json.DefaultJsonProtocol._
          val timer = jobDelete.time()
          val future = daoActor ? GetJobInfo(jobId)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            future.flatMap {
              case None =>
                completeWithErrorStatus(ctx, "No such job ID " + jobId, StatusCodes.NotFound)
              case Some(JobInfo(_, _, contextName, _, classPath, _, None, _, _, _)) =>
                val jobManager = getJobManagerForContext(Some(contextName), config, classPath)
                val future = jobManager.get ? KillJob(jobId)
                future.flatMap {
                  case JobKilled(_, _) => ctx.complete(Map(StatusKey -> JobStatus.Killed))
                }.recoverWith {
                  case e: Exception => completeWithException(
                    ctx, "ERROR", StatusCodes.InternalServerError, e)
                }
              case Some(JobInfo(_, _, _, _, state, _, _, Some(ex), _, _)) if state.equals(JobStatus.Error) =>
                ctx.complete(Map[String, Any](StatusKey -> JobStatus.Error, "ERROR" -> formatException(ex)))
              case Some(JobInfo(_, _, _, _, state, _, _, _, _, _))
                if (state.equals(JobStatus.Finished) || state.equals(JobStatus.Killed)) =>
                completeWithErrorStatus(ctx, "No running job with ID " + jobId, StatusCodes.NotFound)
              case _ => completeWithErrorStatus(
                ctx, "Received an unexpected message", StatusCodes.InternalServerError)
            }.recoverWith {
              case e: Exception =>
                completeWithException(
                  ctx, "ERROR", StatusCodes.InternalServerError, e)
            }.andThen {
              case _ => timer.stop()
            }
          }
        } ~
        /**
         * GET /jobs   -- returns a JSON list of hashes containing job status, ex:
         * [
         *   {
         *     "duration": "Job not done yet",
         *     "classPath": "spark.jobserver.WordCountExample",
         *     "startTime": "2016-06-19T16:27:12.196+05:30",
         *     "context": "b7ea0eb5-spark.jobserver.WordCountExample",
         *     "status": "RUNNING",
         *     "jobId": "5453779a-f004-45fc-a11d-a39dae0f9bf4"
         *   }
         * ]
         * @optional @param limit Int - optional limit to number of jobs to display, defaults to 50
         */
        (get & authorize(authInfo.hasPermission(JOBS_READ))) {
          import spray.json.DefaultJsonProtocol._
          parameters('limit.as[Int] ?, 'status.as[String] ?) { (limitOpt, statusOpt) =>
            val timer = jobGet.time()
            val limit = limitOpt.getOrElse(DefaultJobLimit)
            val statusUpperCaseOpt = statusOpt match {
              case Some(status) => Some(status.toUpperCase())
              case _ => None
            }
            val future = (daoActor ? GetJobInfos(limit, statusUpperCaseOpt)).mapTo[JobInfos]
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              future.flatMap { infos =>
                val jobReport = infos.jobInfos.map { info =>
                  getJobReport(info)
                }
                ctx.complete(jobReport)
              }.recoverWith {
                case e: Exception => completeWithException(
                  ctx, "ERROR", StatusCodes.InternalServerError, e)
              }.andThen {
                case _ => timer.stop()
              }
            }
          }
        } ~
        /**
         * POST /jobs   -- Starts a new job.  The job JAR must have been previously uploaded, and
         *                 the classpath must refer to an object that implements SparkJob.  The `validate()`
         *                 API will be invoked before `runJob`.
         *
         * @entity         The POST entity should be a Typesafe Config format file;
         *                 It will be merged with the job server's config file at startup.
         * @optional @param classPath String - the fully qualified class path for the job (deprecated - use
         *           mainClass parameter)
         * @optional @param appName String - the appName for the job JAR (deprecated - use cp parameter)
         * @optional @param cp String - list of binaries/URIs for the job
         * @optional @param mainClass String - the fully qualified class path for the job
         * @optional @param context String - the name of the context to run the job under.  If not specified,
         *           then a temporary context is allocated for the job
         * @optional @param sync Boolean if "true", then wait for and return results, otherwise return job Id
         * @optional @param timeout Int - the number of seconds to wait for sync results to come back
         * @optional @param callbackUrl - a URL to call with results once the job has finished
         * @return JSON result of { StatusKey -> "OK" | "ERROR", ResultKey -> "result"}, where "result" is
         *         either the job id, or a result
         */
        (post & authorize(authInfo.hasPermission(JOBS_START))) {
          import spray.json.DefaultJsonProtocol._
          entity(as[String]) { configString =>
            parameters('appName ?, 'classPath ?, 'cp ?, 'mainClass ?,
              'context ?, 'sync.as[Boolean] ?, 'timeout.as[Int] ?, SparkJobUtils.SPARK_PROXY_USER_PARAM ?,
              'callbackUrl ?) {
              (appNameOpt, classPathOpt, cpOpt, mainClassOpt,
               contextOpt, syncOpt, timeoutOpt, sparkProxyUser, callbackUrlOpt) =>
                val timer = jobPost.time()
                try {
                  val async = !syncOpt.getOrElse(false)
                  val postedJobConfig = ConfigFactory.parseString(configString)
                  val jobConfig = postedJobConfig.withFallback(config).resolve()
                  val contextConfig = getContextConfig(jobConfig,
                    sparkProxyUser.map(user => Map((SparkJobUtils.SPARK_PROXY_USER_PARAM, user)))
                      .getOrElse(Map.empty))
                  val (cName, cConfig) =
                    determineProxyUser(contextConfig, authInfo, contextOpt.getOrElse(""))
                  val providedMainClass = Try(mainClassOpt.getOrElse(jobConfig.getString("mainClass"))).
                    getOrElse("")
                  val providedCp = if (cpOpt.isEmpty) {
                    Utils.getSeqFromConfig(jobConfig, "cp")
                  } else {
                    cpOpt.get.split(",").toSeq
                  }
                  val callbackOpt: Option[Uri] = callbackUrlOpt
                    .map(URLDecoder.decode(_, "UTF-8"))
                    .map(Uri(_))
                    .map(uri => uri.scheme match {
                      case "http" | "https" => uri
                      case _ => throw new IllegalArgumentException("Callback Url has to be 'http'/'https'")
                    })

                  val (mainClass, cp) = if (appNameOpt.isDefined) {
                    if (classPathOpt.isDefined) {
                      (classPathOpt.get,
                        Utils.getSeqFromConfig(jobConfig, "dependent-jar-uris") ++ Seq(appNameOpt.get))
                    } else {
                      throw InsufficientConfiguration("classPath parameter is missing!")
                    }
                  } else if (providedCp.nonEmpty) {
                    if (providedMainClass.nonEmpty) {
                      (providedMainClass, providedCp)
                    } else {
                      throw InsufficientConfiguration("mainClass parameter is missing!")
                    }
                  } else {
                    throw InsufficientConfiguration("To start the job " +
                      "appName or cp parameters should be configured!")
                  }
                  val timeout = timeoutOpt.map(t => Timeout(t.seconds)).getOrElse(DefaultSyncTimeout)
                  val binManagerFuture = binaryManager.ask(GetBinaryInfoListForCp(cp))(timeout)
                  respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                    binManagerFuture.flatMap {
                      case BinaryInfoListForCp(binInfos) =>
                        logger.info(s"BinaryInfos from BinaryManager: $binInfos")
                        val jobManager = getJobManagerForContext(
                          contextOpt.map(_ => cName), cConfig, mainClass)
                        val events = if (async) asyncEvents else syncEvents
                        val future = jobManager.get.ask(
                          JobManagerActor.StartJob(
                            mainClass, binInfos, jobConfig, events, callbackUrl = callbackOpt))(timeout)
                        future.flatMap {
                          case JobResult(jobId, res) =>
                            res match {
                              case s: Stream[_] => sendStreamingResponse(ctx, ResultChunkSize,
                                resultToByteIterator(Map.empty, s.toIterator))
                              case _ =>
                                ctx.complete(resultToTable(res, Some(jobId)))
                            }
                          case JobErroredOut(jobId, _, ex) =>
                            ctx.complete(exceptionToMap(jobId, ex))
                          case JobStarted(_, jobInfo) =>
                            val future = daoActor ? SaveJobConfig(jobInfo.jobId, postedJobConfig)
                            future.flatMap {
                              case JobConfigStored =>
                                val jobReport = getJobReport(jobInfo, jobStarted = true)
                                ctx.complete(StatusCodes.Accepted, jobReport)
                              case JobConfigStoreFailed =>
                                completeWithErrorStatus(
                                  ctx, "Failed to save job config", StatusCodes.InternalServerError)
                            }.recoverWith {
                              case e: Exception => completeWithException(
                                ctx, "ERROR", StatusCodes.InternalServerError, e)
                            }
                          case JobValidationFailed(_, _, ex) =>
                            completeWithException(ctx, "VALIDATION FAILED", StatusCodes.BadRequest, ex)
                          case NoSuchFile(name) => completeWithErrorStatus(
                            ctx, "appName " + name + " not found", StatusCodes.NotFound)
                          case NoSuchClass => completeWithErrorStatus(ctx,
                              "classPath " + providedMainClass + " not found", StatusCodes.NotFound)
                          case WrongJobType => completeWithErrorStatus(
                            ctx, "Invalid job type for this context", StatusCodes.BadRequest)
                          case JobLoadingError(err) => err match {
                            case _: MalformedURLException => completeWithException(
                              ctx, "JOB LOADING FAILED: Malformed URL", StatusCodes.BadRequest, err)
                            case _ => completeWithException (
                              ctx, "JOB LOADING FAILED", StatusCodes.BadRequest, err)
                          }
                          case ContextStopInProgress =>
                            completeWithErrorStatus(ctx, "Context stop in progress", StatusCodes.Conflict)
                          case NoJobSlotsAvailable(maxJobSlots) =>
                            val errorMsg = "Too many running jobs (" + maxJobSlots.toString +
                              ") for job context '" + contextOpt.getOrElse("ad-hoc") + "'"
                            ctx.complete(
                              StatusCodes.ServiceUnavailable,
                              Map(StatusKey -> "NO SLOTS AVAILABLE", ResultKey -> errorMsg)
                            )
                          case ContextInitError(e) => e match {
                            case _: MalformedURLException => completeWithException(
                              ctx, "CONTEXT INIT ERROR: Malformed URL", StatusCodes.BadRequest, e)
                            case _ => completeWithException(
                              ctx, "CONTEXT INIT FAILED", StatusCodes.InternalServerError, e)
                          }
                        }.recoverWith {
                          case e: Exception => completeWithException(
                            ctx, "ERROR", StatusCodes.InternalServerError, e)
                        }.andThen {
                          case _ => timer.stop()
                        }
                      case NoSuchBinary(name) =>
                        completeWithErrorStatus(ctx, "appName " + name + " not found", StatusCodes.NotFound)
                      case GetBinaryInfoListForCpFailure(ex) =>
                        completeWithException(
                          ctx, "ERROR", StatusCodes.InternalServerError, ex)
                    }.recoverWith {
                      case _: NoSuchElementException =>
                        completeWithErrorStatus(
                          ctx, "context " + contextOpt.get + " not found", StatusCodes.NotFound)
                      case e: Exception => completeWithException(
                        ctx, "ERROR", StatusCodes.InternalServerError, e)
                    }.andThen {
                      case _ => timer.stop()
                    }
                  }
                } catch {
                  case e @ (_: ConfigException | _: InsufficientConfiguration) => timer.stop()
                    respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                      completeWithErrorStatus(
                        ctx, "Cannot parse config: " + e.getMessage, StatusCodes.BadRequest)
                    }
                  case e: Exception =>
                    timer.stop()
                    respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                      completeWithException(ctx, "ERROR", StatusCodes.InternalServerError, e)
                    }
                  }
              }
          }
        }
    })
  }

  def timeoutRoute(ctx: RequestContext): HttpResponse = {
    val route: Route = respondWithMediaType(MediaTypes.`application/json`) { ctx =>
      completeWithErrorStatus(ctx, "Request timed out. Try using the /jobs/<jobID>, " +
        "/jobs APIs to get status/results", StatusCodes.InternalServerError)
    }
    Await.result(route(ctx), 5.seconds) match {
      case Complete(response) => response
      case _ => HttpResponse(StatusCodes.InternalServerError, entity = "Unable to produce timeout error")
    }
  }

  /**
   * if the shiro user is to be used as the proxy user, then this
   * computes the context name from the user name (and a prefix) and appends the user name
   * as the spark proxy user parameter to the config
   */
  def determineProxyUser(aConfig: Config,
                         authInfo: AuthInfo,
                         contextName: String): (String, Config) = {
    if (config.hasPath("access-control.shiro") &&
      config.getBoolean("access-control.shiro.use-as-proxy-user")) {
      //proxy-user-param is ignored and the authenticated user name is used
      val config = aConfig.withValue(SparkJobUtils.SPARK_PROXY_USER_PARAM,
        ConfigValueFactory.fromAnyRef(authInfo.toString))
      (SparkJobUtils.userNamePrefix(authInfo.toString) + contextName, config)
    } else {
      (contextName, aConfig)
    }
  }

  private def getJobManagerForContext(context: Option[String],
                                      contextConfig: Config,
                                      mainClass: String): Option[ActorRef] = {
    import ContextSupervisor._
    val msg = if (context.isDefined) {
        GetContext(context.get)
      } else {
        StartAdHocContext(mainClass, contextConfig)
      }
    val future = (supervisor ? msg)(contextTimeout.seconds)
    Await.result(future, contextTimeout.seconds) match {
      case (manager: ActorRef, global: ActorRef) => Some(manager)
      case (manager: ActorRef) => Some(manager)
      case NoSuchContext => None
      case ContextInitError(err) => throw new RuntimeException(err)
    }
  }

  private def getContextConfig(baseConfig: Config, fallbackParams: Map[String, String] = Map()): Config = {
    import collection.JavaConverters.mapAsJavaMapConverter
    Try(baseConfig.getConfig("spark.context-settings"))
      .getOrElse(ConfigFactory.empty)
      .withFallback(ConfigFactory.parseMap(fallbackParams.asJava))
      .resolve()
  }
}
