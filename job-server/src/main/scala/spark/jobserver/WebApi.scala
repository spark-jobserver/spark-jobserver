package spark.jobserver

import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpCredentials, Location, RawHeader, `Content-Type`}
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.stream.scaladsl
import akka.util.{ByteString, Timeout}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.config._
import javax.net.ssl.SSLContext
import org.apache.shiro.SecurityUtils
import org.apache.shiro.config.IniSecurityManagerFactory
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import spark.jobserver.WebApi.errMap
import spark.jobserver.auth._
import spark.jobserver.common.akka.web.JsonUtils.AnyJsonFormat
import spark.jobserver.common.akka.web.{CommonRoutes, WebService}
import spark.jobserver.io._
import spark.jobserver.routes.DataRoutes
import spark.jobserver.util.{SSLContextFactory, SparkJobUtils}
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.model.HttpEntity.{ChunkStreamPart, Chunked}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object WebApi {
  val StatusKey = "status"
  val ResultKey = "result"
  val ResultKeyStartBytes: Array[Byte] = "{\n".getBytes
  val ResultKeyEndBytes: Array[Byte] = "}".getBytes
  val ResultKeyBytes: Array[Byte] = ("\"" + ResultKey + "\":").getBytes

  val dataToByteStr : Byte => ByteString = b => ByteString(b)


  def badRequest(ctx: RequestContext, msg: String) {
    ctx.complete(StatusCodes.BadRequest, errMap(msg))
  }

  def notFound(ctx: RequestContext, msg: String) {
    ctx.complete(StatusCodes.NotFound, errMap(msg))
  }

  def errMap(errMsg: String): Map[String, String] =
    Map(StatusKey -> JobStatus.Error, ResultKey -> errMsg)

  def errMap(t: Throwable, status: String): Map[String, Any] =
    Map(StatusKey -> status, ResultKey -> formatException(t))

  def successMap(msg: String): Map[String, String] =
    Map(StatusKey -> "SUCCESS", ResultKey -> msg)

  def getJobDurationString(info: JobInfo): String =
    info.jobLengthMillis
      .map { ms => ms / 1000.0 + " secs"
      }
      .getOrElse("Job not done yet")

  def resultToMap(result: Any): Map[String, Any] = result match {
    case m: Map[_, _] => m.map { case (k, v) => (k.toString, v) }
    case s: Seq[_] =>
      s.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case a: Array[_] =>
      a.toSeq.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case item => Map(ResultKey -> item)
  }

  def resultToTable(result: Any): Map[String, Any] = {
    Map(ResultKey -> result)
  }

  def resultToByteIterator(jobReport: Map[String, Any],
                           result: Iterator[_]): Iterator[Byte] = {
    ResultKeyStartBytes.toIterator ++
      (jobReport
        .map(
          t =>
            Seq(
              AnyJsonFormat.write(t._1).toString(),
              AnyJsonFormat.write(t._2).toString()
            ).mkString(":")
        )
        .mkString(",") ++
        (if (jobReport.nonEmpty) "," else "")).getBytes().toIterator ++
      ResultKeyBytes.toIterator ++ result.asInstanceOf[Iterator[Byte]] ++ ResultKeyEndBytes.toIterator
  }

  def formatException(t: Throwable): Any =
    Map(
      "message" -> t.getMessage,
      "errorClass" -> t.getClass.getName,
      "stack" -> ErrorData.getStackTrace(t)
    )

  def formatException(t: ErrorData): Any = {
    Map(
      "message" -> t.message,
      "errorClass" -> t.errorClass,
      "stack" -> t.stackTrace
    )
  }

  def getJobReport(jobInfo: JobInfo,
                   jobStarted: Boolean = false): Map[String, Any] = {

    val statusMap = jobInfo match {
      case JobInfo(_, _, _, _, _, state, _, _, Some(err)) =>
        Map(StatusKey -> state, ResultKey -> formatException(err))
      case JobInfo(_, _, _, _, _, _, _, _, None) if jobStarted =>
        Map(StatusKey -> JobStatus.Started)
      case JobInfo(_, _, _, _, _, state, _, _, None) => Map(StatusKey -> state)
    }
    Map(
      "jobId" -> jobInfo.jobId,
      "startTime" -> jobInfo.startTime.toString(),
      "classPath" -> jobInfo.classPath,
      "context" -> (if (jobInfo.contextName.isEmpty) {"<<ad-hoc>>"}
                    else {jobInfo.contextName}),
      "contextId" -> jobInfo.contextId,
      "duration" -> getJobDurationString(jobInfo)
    ) ++ statusMap
  }

  def getContextReport(context: Any,
                       appId: Option[String],
                       url: Option[String]): Map[String, String] = {
    import scala.collection.mutable
    val map = mutable.Map.empty[String, String]
    context match {
      case contextInfo: ContextInfo =>
        map("id") = contextInfo.id
        map("name") = contextInfo.name
        map("startTime") = contextInfo.startTime.toString()
        map("endTime") =
          if (contextInfo.endTime.isDefined){contextInfo.endTime.get.toString}
          else {"Empty"}
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
}

class WebApi(system: ActorSystem,
             config: Config,
             port: Int,
             binaryManager: ActorRef,
             dataManager: ActorRef,
             supervisor: ActorRef,
             jobInfoActor: ActorRef)
    extends CommonRoutes
    with DataRoutes
    with SJSAuthenticator {
  import CommonMessages._
  import ContextSupervisor._
  import WebApi._

  import scala.concurrent.duration._

  // Get spray-json type classes for serializing Map[String, Any]
  import spark.jobserver.common.akka.web.JsonUtils._

  def actorRefFactory: ActorSystem = system
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val actorSystem: ActorSystem = system
  implicit val ShortTimeout: Timeout =
    Timeout(
      config
        .getDuration("spark.jobserver.short-timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  val DefaultSyncTimeout: FiniteDuration = 10 seconds
  val DefaultJobLimit = 50
  val StatusKey = "status"
  val ResultKey = "result"
  val ResultChunkSize: Int = Option("spark.jobserver.result-chunk-size")
    .filter(config.hasPath)
    .fold(100 * 1024)(config.getBytes(_).toInt)

  val contextTimeout: Int = SparkJobUtils.getContextCreationTimeout(config)
  val contextDeletionTimeout: Int =
    SparkJobUtils.getContextDeletionTimeout(config)
  val bindAddress: String = config.getString("spark.jobserver.bind-address")

  override val logger: Logger = LoggerFactory.getLogger(getClass)

  val rejectionHandler: RejectionHandler = corsRejectionHandler withFallback RejectionHandler.default
  val exceptionHandler = ExceptionHandler {
    case e: Exception =>
      complete(
        StatusCodes.InternalServerError,
        errMap(e, "ERROR")
      )
  }

  val handleErrors: Directive0 = handleRejections(rejectionHandler) & handleExceptions(exceptionHandler)

  val myRoutes: Route = cors() {
    handleErrors {
      overrideMethodWithParameter("_method") {
        binaryRoutes ~ jarRoutes ~ contextRoutes ~ jobRoutes ~
          dataRoutes ~ healthzRoutes ~ otherRoutes
      }
    }
  }

  lazy val authenticator
    : Option[HttpCredentials] => Future[AuthenticationResult[User]] = {
    if (config.getBoolean("shiro.authentication")) {
      import java.util.concurrent.TimeUnit
      logger.info("Using authentication.")
      initSecurityManager()
      val authTimeout = Try(
        config
          .getDuration("shiro.authentication-timeout", TimeUnit.MILLISECONDS)
          .toInt / 1000
      ).getOrElse(10)
      asShiroAuthenticator(authTimeout)
    } else {
      logger.info("No authentication.")
      asAllUserAuthenticator
    }
  }

  /**
    * possibly overwritten by test
    */
  def initSecurityManager() {
    val sManager = new IniSecurityManagerFactory(
      config.getString("shiro.config.path")
    ).getInstance()
    SecurityUtils.setSecurityManager(sManager)
  }

  def start() {

    /**
      * activates ssl or tsl encryption between client and SJS if so requested
      * in config
      */
    implicit val sslContext: SSLContext = {
      SSLContextFactory.createContext(config.getConfig("akka.http.server"))
    }

    logger.info("Starting browser web service...")
    WebService.start(myRoutes ~ commonRoutes, system, bindAddress, port)
  }

  def extracContentTypeHeader: HttpHeader => Option[ContentType] = {
    case HttpHeader("Content-Type", value) => Some(ContentType.parse(value)) match {
      case Some(Right(v)) => Some(v)
      case Some(Left(v)) => throw new IllegalArgumentException(v.mkString)
    }
    case h: RawHeader if h.name == "Content-Type" => Some(ContentType.parse(h.value)) match {
      case Some(Right(v)) => Some(v)
      case Some(Left(v)) => throw new IllegalArgumentException(v.mkString)
    }
    case _ => None
  }

  val contentType: Directive1[Option[ContentType]] = optionalHeaderValue(extracContentTypeHeader)

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
  def binaryRoutes: Route = pathPrefix("binaries") {
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

  /**
    * Routes for listing and uploading jars
    *    GET /jars              - lists all current jars
    *    POST /jars/<appName>   - upload a new jar file
    *
    * NB these routes are kept for legacy purposes but are deprecated in favour
    *  of the /binaries routes.
    */
  def jarRoutes: Route = pathPrefix("jars") {
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

  /**
    * Routes for listing, deletion of and storing data files
    *    GET /data                     - lists all currently stored files
    *    DELETE /data/<filename>       - deletes given file, no-op if file does not exist
    *    POST /data/<filename-prefix>  - upload a new data file, using the given prefix,
    *                                      a time stamp is appended to ensure uniqueness
    *
    * @author TimMaltGermany
    */
  def dataRoutes: Route = pathPrefix("data") {
    // user authentication
    authenticateOrRejectWithChallenge(authenticator) { _ =>
      dataRoutes(dataManager)
    }
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
                    .getDuration(
                      "spark.jobserver.context-lookup-timeout",
                      TimeUnit.MILLISECONDS
                    )
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

  /**
    * Routes for getting health status of job server
    *    GET /healthz              - return OK or error message
    */
  def healthzRoutes: Route = pathPrefix("healthz") {
    //no authentication required
    get {
      logger.info("Receiving healthz check request")
      complete("OK")
    }
  }

  def otherRoutes: Route = {
    import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
//    implicit val ar = actorRefFactory
    //no authentication required

    path("") {
      // Main index.html page
      getFromResource("html/index.html")
    } ~ pathPrefix("html") {
      // Static files needed by index.html
      getFromResourceDirectory("html")
    }
  }

  val errorEvents: Set[Class[_]] =
    Set(classOf[JobErroredOut], classOf[JobValidationFailed])
  val asyncEvents: Set[Class[_]] = Set(classOf[JobStarted]) ++ errorEvents
  val syncEvents: Set[Class[_]] = Set(classOf[JobResult]) ++ errorEvents

  /**
    * Main routes for starting a job, listing existing jobs, getting job results
    */
  def jobRoutes: Route = pathPrefix("jobs") {
    import JobInfoActor._
    import JobManagerActor._

    // user authentication
    authenticateOrRejectWithChallenge(authenticator) { user =>
      /**
        * GET /jobs/<jobId>/config --
        * returns the configuration used to launch this job or an error if not found.
        *
        * @required @param jobId
        */
      (get & path(Segment / "config")) { jobId =>
        val renderOptions = ConfigRenderOptions
          .defaults()
          .setComments(false)
          .setOriginComments(false)

        onComplete(jobInfoActor ? GetJobConfig(jobId)) {
          case Success(value) =>
            value match {
              case NoSuchJobId =>
                complete(StatusCodes.NotFound, errMap(s"No such job ID $jobId"))
              case cnf: Config =>
                complete(cnf.root().render(renderOptions))
            }
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
        }
      } ~
        // GET /jobs/<jobId>
        // Returns job information in JSON.
        // If the job isn't finished yet, then {"status": "RUNNING" | "ERROR"} is returned.
        // Returned JSON contains result attribute if status is "FINISHED"
        (get & path(Segment)) { jobId =>
          onComplete(jobInfoActor ? GetJobStatus(jobId)) {
            case Success(value) =>
              value match {
                case NoSuchJobId =>
                  complete(
                    StatusCodes.NotFound,
                    errMap(s"No such job ID $jobId")
                  )
                case info: JobInfo =>
                  val jobReport = getJobReport(info)
                  onComplete(jobInfoActor ? GetJobResult(jobId)) {
                    case Success(v) =>
                      v match {
                        case JobResult(_, result) =>
                          result match {
                            case s: Stream[_] =>

                              implicit val jsonStreamingSupport =
                                EntityStreamingSupport.json()
                              val r = resultToByteIterator(jobReport,
                                                           s.toIterator)

                              val chunks = scaladsl.Source
                                .fromIterator(() => r)
                                .map(dataToByteStr)
                                .map(ChunkStreamPart(_))

                              val entity : ResponseEntity =
                                Chunked(ContentTypes.`application/json`, chunks)

                              val httpResponse : HttpResponse =
                                HttpResponse(entity = entity)

                              complete(httpResponse)

                            case _ =>
                              complete(jobReport ++ resultToTable(result))
                          }
                        case _ =>
                          complete(jobReport)
                      }
                    case Failure(ex) =>
                      complete(
                        StatusCodes.InternalServerError,
                        errMap(ex, "ERROR")
                      )
                  }
              }
            case Failure(ex) =>
              complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
          }
        } ~
        //  DELETE /jobs/<jobId>
        //  Stop the current job. All other jobs submitted with this spark context
        //  will continue to run
        (delete & path(Segment)) { jobId =>
          onComplete(jobInfoActor ? GetJobStatus(jobId)) {
            case Success(value) =>
              value match {
                case NoSuchJobId =>
                  complete(
                    StatusCodes.NotFound,
                    errMap(s"No such job ID $jobId")
                  )
                case JobInfo(_, _, contextName, _, _, classPath, _, None, _) =>
                  val jobManager = getJobManagerForContext(
                    Some(contextName),
                    config,
                    classPath
                  )
                  onComplete(jobManager.get ? KillJob(jobId)) {
                    case Success(v) =>
                      v match {
                        case JobKilled(_, _) =>
                          complete(Map(StatusKey -> JobStatus.Killed))
                      }
                    case Failure(ex) =>
                      complete(
                        StatusCodes.InternalServerError,
                        errMap(ex, "ERROR")
                      )
                  }
                case JobInfo(_, _, _, _, _, state, _, _, Some(ex))
                    if state.equals(JobStatus.Error) =>
                  complete(
                    Map(
                      StatusKey -> JobStatus.Error,
                      "ERROR" -> formatException(ex)
                    )
                  )
                case JobInfo(_, _, _, _, _, state, _, _, _)
                    if state.equals(JobStatus.Finished) || state
                      .equals(JobStatus.Killed) =>
                  complete(
                    StatusCodes.NotFound,
                    errMap(s"No such job ID $jobId")
                  )
                case _ =>
                  complete(
                    StatusCodes.InternalServerError,
                    errMap("Received an unexpected message")
                  )
              }
            case Failure(ex) =>
              complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
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
          *
          * @optional @param limit Int - optional limit to number of jobs to display, defaults to 50
          */
        get {
          parameters('limit.as[Int] ?, 'status.as[String] ?) {
            (limitOpt, statusOpt) =>
              val limit = limitOpt.getOrElse(DefaultJobLimit)
              val statusUpperCaseOpt = statusOpt match {
                case Some(status) =>
                  Some(status.toUpperCase())
                case _ =>
                  None
              }

              onComplete(
                (jobInfoActor ? GetJobStatuses(Some(limit), statusUpperCaseOpt))
                  .mapTo[Seq[JobInfo]]
              ) {
                case Success(infos) =>
                  val jobReport = infos.map(getJobReport(_))
                  complete(jobReport)
                case Failure(ex) =>
                  complete(StatusCodes.InternalServerError, errMap(ex, "ERROR"))
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
          * @required @param appName String - the appName for the job JAR
          * @required @param classPath String - the fully qualified class path for the job
          * @optional @param context String - the name of the context to run the job under.  If not specified,
          *                                   then a temporary context is allocated for the job
          * @optional @param sync Boolean if "true", then wait for and return results, otherwise return job Id
          * @optional @param timeout Int - the number of seconds to wait for sync results to come back
          * @return JSON result of { StatusKey -> "OK" | "ERROR", ResultKey -> "result"}, where "result" is
          *         either the job id, or a result
          */
        post {
          entity(as[String]) { configString =>
            JobsRouteUtils.exceptionHandler {
              parameters(
                'appName,
                'classPath,
                'context ?,
                'sync.as[Boolean] ?,
                'timeout.as[Int] ?,
                SparkJobUtils.SPARK_PROXY_USER_PARAM ?
              ) {
                (appName,
                 classPath,
                 contextOpt,
                 syncOpt,
                 timeoutOpt,
                 sparkProxyUser) =>
                  val async = !syncOpt.getOrElse(false)
                  val postedJobConfig = ConfigFactory.parseString(configString)
                  val jobConfig = postedJobConfig.withFallback(config).resolve()
                  val contextConfig = getContextConfig(
                    jobConfig,
                    sparkProxyUser
                      .map(user => Map((SparkJobUtils.SPARK_PROXY_USER_PARAM, user)))
                      .getOrElse(Map.empty)
                  )
                  val (cName, cConfig) =
                    determineProxyUser(
                      contextConfig,
                      AuthInfo(user),
                      contextOpt.getOrElse("")
                    )
                  val jobManager = getJobManagerForContext(
                    contextOpt.map(_ => cName),
                    cConfig,
                    classPath
                  )
                  val events = if (async) asyncEvents else syncEvents
                  val timeout =
                    timeoutOpt.map(t => t.seconds).getOrElse(DefaultSyncTimeout)
                  withRequestTimeout(timeout) {
                    onComplete(
                      jobManager.get ? JobManagerActor
                        .StartJob(appName, classPath, jobConfig, events)
                    ) {
                      case Success(value) =>
                        value match {
                          case JobResult(jobId, res) =>
                            res match {
                              case s: Stream[_] =>

                                implicit val jsonStreamingSupport =
                                  EntityStreamingSupport.json()
                                    val r = resultToByteIterator(Map.empty, s.toIterator)

                                    val chunks = scaladsl.Source
                                      .fromIterator(() => r)
                                      .map(dataToByteStr)
                                      .map(ChunkStreamPart(_))

                                val entity : ResponseEntity =
                                  Chunked(ContentTypes.`application/json`, chunks)

                                val httpResponse : HttpResponse =
                                  HttpResponse(entity = entity)

                                    complete(httpResponse)
                              case _ =>
                                complete(
                                  Map[String, Any]("jobId" -> jobId) ++ resultToTable(
                                    res
                                  )
                                )
                            }
                          case JobErroredOut(jobId, _, ex) =>
                            complete(
                              Map[String, String]("jobId" -> jobId) ++ errMap(
                                ex,
                                "ERROR"
                              )
                            )
                          case JobStarted(_, jobInfo) =>
                            onComplete(
                              jobInfoActor ? StoreJobConfig(
                                jobInfo.jobId,
                                postedJobConfig
                              )
                            ) {
                              case Success(v) =>
                                v match {
                                  case JobConfigStored =>
                                    complete(
                                      StatusCodes.Accepted,
                                      getJobReport(jobInfo, jobStarted = true)
                                    )
                                }
                              case Failure(ex) =>
                                complete(
                                  StatusCodes.InternalServerError,
                                  errMap(ex, "ERROR")
                                )
                            }
                          case JobValidationFailed(_, _, ex) =>
                            complete(
                              StatusCodes.BadRequest,
                              errMap(ex, "VALIDATION FAILED")
                            )
                          case NoSuchApplication =>
                            complete(
                              StatusCodes.NotFound,
                              errMap(s"appName $appName not found")
                            )
                          case NoSuchClass =>
                            complete(
                              StatusCodes.NotFound,
                              errMap(s"classPath $classPath not found")
                            )
                          case WrongJobType =>
                            complete(
                              StatusCodes.BadRequest,
                              errMap("Invalid job type for this context")
                            )
                          case JobLoadingError(err) =>
                            complete(
                              StatusCodes.InternalServerError,
                              errMap(err, "JOB LOADING FAILED")
                            )
                          case ContextStopInProgress =>
                            complete(
                              StatusCodes.Conflict,
                              errMap("Context stop in progress")
                            )
                          case NoJobSlotsAvailable(maxJobSlots) =>
                            val errorMsg =
                              s"Too many running jobs (${maxJobSlots.toString}) for job context '${contextOpt
                                .getOrElse("ad-hoc")}'"
                            complete(
                              StatusCodes.ServiceUnavailable,
                              Map(
                                StatusKey -> "NO SLOTS AVAILABLE",
                                ResultKey -> errorMsg
                              )
                            )
                          case ContextInitError(e) =>
                            complete(
                              StatusCodes.InternalServerError,
                              errMap(e, "CONTEXT INIT FAILED")
                            )
                        }
                      case Failure(ex) =>
                        ex match {
                          case e: Exception =>
                            complete(
                              StatusCodes.InternalServerError,
                              errMap(e, "ERROR")
                            )
                        }
                    }
                  }
              }
            }
          }
        }
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
    if (config.getBoolean("shiro.authentication") && config.getBoolean(
          "shiro.use-as-proxy-user"
        )) {
      //proxy-user-param is ignored and the authenticated user name is used
      val config = aConfig.withValue(
        SparkJobUtils.SPARK_PROXY_USER_PARAM,
        ConfigValueFactory.fromAnyRef(authInfo.toString)
      )
      (SparkJobUtils.userNamePrefix(authInfo.toString) + contextName, config)
    } else {
      (contextName, aConfig)
    }
  }

  private def getJobManagerForContext(context: Option[String],
                                      contextConfig: Config,
                                      classPath: String): Option[ActorRef] = {
    import ContextSupervisor._
    val msg =
      if (context.isDefined) {
        GetContext(context.get)
      } else {
        StartAdHocContext(classPath, contextConfig)
      }
    val future = (supervisor ? msg)(contextTimeout.seconds)
    Await.result(future, contextTimeout.seconds) match {
      case manager: ActorRef     => Some(manager)
      case NoSuchContext         => None
      case ContextInitError(err) => throw new RuntimeException(err)
    }
  }

  private def getContextConfig(
    baseConfig: Config,
    fallbackParams: Map[String, String] = Map()
  ): Config = {
    import collection.JavaConverters.mapAsJavaMapConverter
    Try(baseConfig.getConfig("spark.context-settings"))
      .getOrElse(ConfigFactory.empty)
      .withFallback(ConfigFactory.parseMap(fallbackParams.asJava))
      .resolve()
  }

}

object JobsRouteUtils {
  private val _exceptionHandler = ExceptionHandler {
    case e: NoSuchElementException =>
      complete(
        StatusCodes.NotFound,
        errMap("context " + "contextOpt.get" + " not found")
      )
    case e: ConfigException =>
      complete(
        StatusCodes.BadRequest,
        errMap("Cannot parse config: " + e.getMessage)
      )
  }

  val exceptionHandler: Directive0 = handleExceptions(_exceptionHandler)
}