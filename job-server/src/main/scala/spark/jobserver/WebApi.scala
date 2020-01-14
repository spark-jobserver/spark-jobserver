package spark.jobserver

import java.net.MalformedURLException
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit

import javax.net.ssl.SSLContext
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigException, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import spark.jobserver.common.akka.web.JsonUtils.AnyJsonFormat
import spark.jobserver.common.akka.web.{CommonRoutes, WebService}
import org.apache.shiro.SecurityUtils
import org.apache.shiro.config.IniSecurityManagerFactory
import org.apache.http.HttpStatus
import org.joda.time.DateTime
import spark.jobserver.auth._
import spark.jobserver.io.{BinaryType, ContextInfo, ErrorData, JobInfo, JobStatus, JobDAOActor}
import spark.jobserver.routes.DataRoutes
import spark.jobserver.util.{HealthCheck, InsufficientConfiguration, MeteredHttpService,
  SSLContextFactory, SparkJobUtils, Utils}

import spray.http.HttpHeaders.{Location, `Content-Type`}
import spray.http._
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.io.ServerSSLEngineProvider
import spray.json.DefaultJsonProtocol._
import spray.routing.directives.{AuthMagnet, LoggingMagnet}
import spray.routing.{RequestContext, Route}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import org.slf4j.LoggerFactory
import spark.jobserver.io.JobDAOActor.LastBinaryInfo

object WebApi {

  val StatusKey = "status"
  val ResultKey = "result"
  val ResultKeyStartBytes = "{\n".getBytes
  val ResultKeyEndBytes = "}".getBytes
  val ResultKeyBytes = ("\"" + ResultKey + "\":").getBytes
  val logger = LoggerFactory.getLogger(getClass)

  def badRequest(ctx: RequestContext, msg: String) {
    ctx.complete(StatusCodes.BadRequest, errMap(msg))
  }

  def notFound(ctx: RequestContext, msg: String) {
    ctx.complete(StatusCodes.NotFound, errMap(msg))
  }

  def logAndComplete(ctx: RequestContext, errMsg: String, stcode: Int) {
    logger.info("StatusCode: " + stcode + ", ErrorMessage: " + errMsg)
    ctx.complete(stcode, errMap(errMsg))
  }

  def logAndComplete(ctx: RequestContext, errMsg: String, statusCode: StatusCode) {
    logAndComplete(ctx, errMsg, statusCode.intValue)
  }

  def logAndComplete(ctx: RequestContext, errMsg: String, stcode: Int, e: Throwable) {
    logger.info("StatusCode: " + stcode + ", ErrorMessage: " + errMsg + ", StackTrace: "
      + ErrorData.getStackTrace(e))
    ctx.complete(stcode, errMap(e, errMsg))
  }

  def errMap(errMsg: String) : Map[String, String] = Map(StatusKey -> JobStatus.Error, ResultKey -> errMsg)

  def errMap(t: Throwable, status: String) : Map[String, Any] =
    Map(StatusKey -> status, ResultKey -> formatException(t))

  def successMap(msg: String): Map[String, String] = Map(StatusKey -> "SUCCESS", ResultKey -> msg)

  def getJobDurationString(info: JobInfo): String =
    info.jobLengthMillis.map { ms => ms / 1000.0 + " secs" }.getOrElse("Job not done yet")

  def resultToMap(result: Any): Map[String, Any] = result match {
    case m: Map[_, _] => m.map { case (k, v) => (k.toString, v) }.toMap
    case s: Seq[_] => s.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case a: Array[_] => a.toSeq.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case item => Map(ResultKey -> item)
  }

  def resultToTable(result: Any): Map[String, Any] = {
    Map(ResultKey -> result)
  }

  def resultToByteIterator(jobReport: Map[String, Any], result: Iterator[_]): Iterator[_] = {
    ResultKeyStartBytes.toIterator ++
      (jobReport.map(t => Seq(AnyJsonFormat.write(t._1).toString(),
                AnyJsonFormat.write(t._2).toString()).mkString(":") ).mkString(",") ++
        (if (jobReport.nonEmpty) "," else "")).getBytes().toIterator ++
      ResultKeyBytes.toIterator ++ result ++ ResultKeyEndBytes.toIterator
  }

  def formatException(t: Throwable): Any =
    Map("message" -> t.getMessage,
      "errorClass" -> t.getClass.getName,
      "stack" -> ErrorData.getStackTrace(t))

  def formatException(t: ErrorData): Any = {
    Map("message" -> t.message,
      "errorClass" -> t.errorClass,
      "stack" -> t.stackTrace
    )
  }

  def getJobReport(jobInfo: JobInfo, jobStarted: Boolean = false): Map[String, Any] = {

    val statusMap = jobInfo match {
      case JobInfo(_, _, _, _, state, _, _, Some(err), _) =>
        Map(StatusKey -> state, ResultKey -> formatException(err))
      case JobInfo(_, _, _, _, _, _, _, None, _) if jobStarted => Map(StatusKey -> JobStatus.Started)
      case JobInfo(_, _, _, _, state, _, _, None, _) => Map(StatusKey -> state)
    }
    Map("jobId" -> jobInfo.jobId,
      "startTime" -> jobInfo.startTime.toString(),
      "classPath" -> jobInfo.mainClass,
      "context" -> (if (jobInfo.contextName.isEmpty) "<<ad-hoc>>" else jobInfo.contextName),
      "contextId" -> jobInfo.contextId,
      "duration" -> getJobDurationString(jobInfo)) ++ statusMap
  }

  def getContextReport(context: Any, appId: Option[String], url: Option[String]): Map[String, String] = {
    import scala.collection.mutable
    val map = mutable.Map.empty[String, String]
    context match {
      case contextInfo: ContextInfo =>
        map("id") = contextInfo.id
        map("name") = contextInfo.name
        map("startTime") = contextInfo.startTime.toString()
        map("endTime") = if (contextInfo.endTime.isDefined) contextInfo.endTime.get.toString else "Empty"
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
             jobInfoActor: ActorRef,
             healthCheckInst: HealthCheck)
    extends MeteredHttpService with CommonRoutes with DataRoutes with SJSAuthenticator with CORSSupport
                        with ChunkEncodedStreamingSupport {
  import CommonMessages._
  import ContextSupervisor._
  import scala.concurrent.duration._
  import WebApi._

  // Get spray-json type classes for serializing Map[String, Any]
  import spark.jobserver.common.akka.web.JsonUtils._

  override def actorRefFactory: ActorSystem = system
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val ShortTimeout =
    Timeout(config.getDuration("spark.jobserver.short-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  val DefaultSyncTimeout = Timeout(10 seconds)
  val DefaultBinaryDeletionTimeout = Timeout(10.seconds)
  val DefaultJobLimit = 50
  val StatusKey = "status"
  val ResultKey = "result"
  val ResultChunkSize = Option("spark.jobserver.result-chunk-size").filter(config.hasPath)
      .fold(100 * 1024)(config.getBytes(_).toInt)

  val contextTimeout = SparkJobUtils.getContextCreationTimeout(config)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  val bindAddress = config.getString("spark.jobserver.bind-address")

  private val maxSprayLogMessageLength = 400

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

  val accessLogger: LoggingMagnet[HttpRequest => Any => Unit] = LoggingMagnet {
    request: HttpRequest =>
      logger.info(s"[${request.method}] ${request.uri}")
      _ match {
        case res: HttpResponse =>
          val resMessage = res.entity.toString.
            replace('\n', ' ').replaceAll(" +", " ")
          logger.info(s"Response: ${res.status} " +
            s"${resMessage.substring(0, Math.min(resMessage.length(), maxSprayLogMessageLength))}(...) " +
            s"to request: ${request.uri}")
        case _ => logger.info("Unknown response")
      }
  }

  val logger = LoggerFactory.getLogger(getClass)

  val myRoutes = logRequestResponse(accessLogger) {
    cors {
      overrideMethodWithParameter("_method") {
        binaryRoutes ~ contextRoutes ~ jobRoutes ~
          dataRoutes ~ healthzRoutes ~ otherRoutes
      }
    }
  }

  lazy val authenticator: AuthMagnet[AuthInfo] = {
    if (config.getBoolean("shiro.authentication")) {
      import java.util.concurrent.TimeUnit
      logger.info("Using authentication.")
      initSecurityManager()
      val authTimeout = Try(config.getDuration("shiro.authentication-timeout",
              TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(10)
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
    val sManager = new IniSecurityManagerFactory(config.getString("shiro.config.path")).getInstance()
    SecurityUtils.setSecurityManager(sManager)
  }

  def start() {

    /**
     * activates ssl or tsl encryption between client and SJS if so requested
     * in config
     */
    implicit val sslContext: SSLContext = {
      SSLContextFactory.createContext(config.getConfig("spray.can.server"))
    }

    implicit def sslEngineProvider: ServerSSLEngineProvider = {
      ServerSSLEngineProvider { engine =>
        val protocols = config.getStringList("spray.can.server.enabledProtocols")
        engine.setEnabledProtocols(protocols.toArray(Array[String]()))
        val sprayConfig = config.getConfig("spray.can.server")
        if(sprayConfig.hasPath("truststore")) {
          engine.setNeedClientAuth(true)
          logger.info("Client authentication activated.")
        }
        engine
      }
    }

    logger.info("Starting browser web service...")
    WebService.start(myRoutes ~ commonRoutes, system, bindAddress, port)
  }

  val contentType =
    optionalHeaderValue {
      case `Content-Type`(ct) => Some(ct)
      case _ => None
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
    * `BinaryType` trait. e.g. "application/java-archive" or
    * application/python-archive" (may be expanded to support other types
    * in future.
    *
    */
  def binaryRoutes: Route = pathPrefix("binaries") {
    // user authentication
    authenticate(authenticator) { authInfo =>
      // GET /binaries/<appName>
      (get & path(Segment)) { appName =>
        val timer = binGetSingle.time()
        respondWithMediaType(MediaTypes.`application/json`) { ctx =>
          val future = (binaryManager ? GetBinary(appName)).mapTo[LastBinaryInfo]
          future.map{
            case LastBinaryInfo(Some(bin)) =>
              val res = Map("app-name" -> bin.appName,
                  "binary-type" -> bin.binaryType.name,
                  "upload-time" -> bin.uploadTime.toString())
              ctx.complete(200, res)
            case LastBinaryInfo(None) => notFound(ctx, s"Can't find binary with name $appName")
            case _ => logAndComplete(ctx, "UNEXPECTED ERROR OCCURRED", 500)
          }.recover {
            case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
          }.andThen {
            case _ => timer.stop()
          }
        }
      } ~
      // GET /binaries route returns a JSON map of the app name
      // and the type of and last upload time of a binary.
      get { ctx =>
        val timer = binGet.time()
        val future = (binaryManager ? ListBinaries(None)).
          mapTo[collection.Map[String, (BinaryType, DateTime)]]
        future.map { binTimeMap =>
          val stringTimeMap = binTimeMap.map {
            case (app, (binType, dt)) =>
              (app, Map("binary-type" -> binType.name, "upload-time" -> dt.toString()))
          }.toMap
          ctx.complete(stringTimeMap)
        }.recover {
          case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
        }.andThen {
          case _ => timer.stop()
        }
      } ~
      // POST /binaries/<appName>
      // The <appName> needs to be unique; uploading a jar with the same appName will replace it.
      // requires a recognised content-type header
      post {
        path(Segment) { appName =>
          val timer = binPost.time()
          entity(as[Array[Byte]]) { binBytes =>
            contentType {
              case Some(x) =>
                respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                  BinaryType.fromMediaType(x.mediaType) match {
                    case Some(binaryType) =>
                      val future = binaryManager ? StoreBinary(appName, binaryType, binBytes)

                      future.map {
                        case BinaryStored =>
                          ctx.complete(StatusCodes.OK)
                        case InvalidBinary => badRequest(ctx, "Binary is not of the right format")
                        case BinaryStorageFailure(ex) => logAndComplete(ctx, "Storage Failure", 500, ex)
                      }.recover {
                        case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
                      }.andThen {
                        case _ => timer.stop()
                      }
                    case None =>
                        logAndComplete(ctx, s"Unsupported binary type $x", 415)
                        timer.stop()
                  }
                }
              case None =>
                timer.stop()
                complete(415, s"Content-Type header must be set to indicate binary type")
            }
          }
        }
      } ~
      // DELETE /binaries/<appName>
      delete {
        path(Segment) { appName =>
          val timer = binDelete.time()
          val future = (binaryManager ? DeleteBinary(appName))(DefaultBinaryDeletionTimeout)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            future.map {
              case BinaryDeleted =>
                ctx.complete(StatusCodes.OK)
              case BinaryInUse(jobs) =>
                logAndComplete(ctx,
                              s"Binary is in use by job(s): ${jobs.mkString(", ")}",
                              StatusCodes.Forbidden)
              case NoSuchBinary(name) => notFound(ctx, s"can't find binary with name $name")
              case BinaryDeletionFailure(ex) =>
                logAndComplete(ctx,
                              s"Failed to delete binary due to internal error. Check logs.",
                              StatusCodes.InternalServerError)
            }.recover {
              case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
            }.andThen {
              case _ => timer.stop()
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
   * @author TimMaltGermany
   */
  def dataRoutes: Route = pathPrefix("data") {
    // user authentication
    authenticate(authenticator) { authInfo =>
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
    authenticate(authenticator) { authInfo =>
      (get & path(Segment)) { contextName =>
        val timer = contextGetSingle.time()
        respondWithMediaType(MediaTypes.`application/json`) { ctx =>
          val future = (supervisor ? GetSparkContexData(contextName))(15.seconds)
          future.map {
            case SparkContexData(context, appId, url) =>
              val contextMap = getContextReport(context, appId, url)
              ctx.complete(200, contextMap)
            case NoSuchContext => notFound(ctx, s"can't find context with name $contextName")
            case UnexpectedError => logAndComplete(ctx, "UNEXPECTED ERROR OCCURRED", 500)
          }.recover {
            case e: Exception =>
              logAndComplete(ctx, "ERROR", 500, e)
          }.andThen{
            case _ => timer.stop()
          }
        }
      } ~
      get { ctx =>
        val timer = contextGet.time()
        val future = supervisor ? ListContexts
        future.map {
          case UnexpectedError => logAndComplete(ctx, "UNEXPECTED ERROR OCCURRED", 500)
          case contexts =>
            val getContexts = SparkJobUtils.removeProxyUserPrefix(
              authInfo.toString, contexts.asInstanceOf[Seq[String]],
              config.getBoolean("shiro.authentication") && config.getBoolean("shiro.use-as-proxy-user"))
            ctx.complete(getContexts)
        }.recover {
          case e: Exception =>
            logAndComplete(ctx, "ERROR", 500, e)
        }.andThen {
          case _ => timer.stop()
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
          path(Segment) { contextName =>
            val timer = contextPost.time()
            // Enforce user context name to start with letters
            if (!contextName.head.isLetter) {
              timer.stop()
              complete(StatusCodes.BadRequest, errMap("context name must start with letters"))
            } else {
              parameterMap { (params) =>
                // parse the config from the body, using url params as fallback values
                val baseConfig = ConfigFactory.parseString(configString)
                val contextConfig = getContextConfig(baseConfig, params)
                val (cName, config) = determineProxyUser(contextConfig, authInfo, contextName)
                val future = (supervisor ? AddContext(cName, config))(contextTimeout.seconds)
                respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                  future.map {
                    case ContextInitialized => ctx.complete(StatusCodes.OK,
                      successMap("Context initialized"))
                    case ContextAlreadyExists => badRequest(ctx, "context " + contextName + " exists")
                    case ContextInitError(e) => e match {
                      case _: MalformedURLException => logAndComplete(
                        ctx, "CONTEXT INIT ERROR: Malformed URL", StatusCodes.BadRequest.intValue, e)
                      case _ => logAndComplete(ctx, "CONTEXT INIT ERROR", 500, e)
                    }
                    case UnexpectedError => logAndComplete(ctx, "UNEXPECTED ERROR OCCURRED", 500)
                  }.recover {
                    case e: Exception =>
                      logAndComplete(ctx, "ERROR", 500, e)
                  }.andThen {
                    case _ => timer.stop()
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
          parameters('force.as[Boolean] ?) {
            forceOpt => {
              val force = forceOpt.getOrElse(false)
              val timer = contextDelete.time()
              val (cName, _) = determineProxyUser(config, authInfo, contextName)
              val future = (supervisor ?
                StopContext(cName, force))(contextDeletionTimeout.seconds + 1.seconds)
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case ContextStopped =>
                    ctx.complete(StatusCodes.OK, successMap("Context stopped"))
                  case ContextStopInProgress =>
                    val response = HttpResponse(
                      status = StatusCodes.Accepted, headers = List(Location(ctx.request.uri)))
                    ctx.complete(response)
                  case NoSuchContext => notFound(ctx, "context " + contextName + " not found")
                  case ContextStopError(e) => logAndComplete(ctx, "CONTEXT DELETE ERROR", 500, e)
                  case UnexpectedError => logAndComplete(ctx, "UNEXPECTED ERROR OCCURRED", 500)
                }.recover {
                  case e: Exception =>
                    logAndComplete(ctx, "ERROR", 500, e)
                }.andThen {
                  case _ => timer.stop()
                }
              }
            }
          }
        } ~
        put {
          parameters("reset", 'sync.as[Boolean] ?) { (reset, sync) =>
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              val timer = contextPut.time()
              reset match {
                case "reboot" =>
                  import ContextSupervisor._
                  import java.util.concurrent.TimeUnit

                  val future = (supervisor ? ListContexts).mapTo[Seq[String]]
                  val lookupTimeout = Try(config.getDuration("spark.jobserver.context-lookup-timeout",
                    TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(1)
                  val contexts = Await.result(future, lookupTimeout.seconds)

                  if (sync.isDefined && !sync.get) {
                    contexts.map(c => supervisor ! StopContext(c))
                    ctx.complete(StatusCodes.OK, successMap("Context reset requested"))
                  } else {
                    val stopFutures = contexts.map(c => supervisor ? StopContext(c))
                    Await.ready(Future.sequence(stopFutures), contextTimeout.seconds)

                    Thread.sleep(1000) // we apparently need some sleeping in here, so spark can catch up

                    (supervisor ? AddContextsFromConfig).onFailure {
                      case t => ctx.complete("ERROR")
                    }
                    ctx.complete(StatusCodes.OK, successMap("Context reset"))
                  }
                timer.stop()
                case _ =>
                  ctx.complete("ERROR")
                  timer.stop()
              }
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
    get { ctx =>
      try {
        if (healthCheckInst != null && healthCheckInst.isHealthy()) {
          ctx.complete(StatusCodes.OK)
        } else {
          ctx.complete(StatusCodes.InternalServerError, errMap("Required actors not alive"))
        }
      }
      catch {
        case ex: Exception => {
          logger.error("Exception in healthz", ex)
          ctx.complete(StatusCodes.InternalServerError, errMap("Exception while invoking health check"))
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
    import JobInfoActor._
    import JobManagerActor._

    // user authentication
    authenticate(authenticator) { authInfo =>
      /**
       * GET /jobs/<jobId>/config --
       * returns the configuration used to launch this job or an error if not found.
       *
       * @required @param jobId
       */
      (get & path(Segment / "config")) { jobId =>
        val timer = configGet.time()
        val renderOptions = ConfigRenderOptions.defaults().setComments(false).setOriginComments(false)

        val future = jobInfoActor ? GetJobConfig(jobId)
        respondWithMediaType(MediaTypes.`application/json`) { ctx =>
          future.map {
            case NoSuchJobId =>
              notFound(ctx, "No such job ID " + jobId)
            case cnf: Config =>
              ctx.complete(cnf.root().render(renderOptions))
          }.recover {
            case e: Exception =>
              logAndComplete(ctx, "ERROR", 500, e)
          }.andThen {
            case _ => timer.stop()
          }
        }
      } ~
        // GET /jobs/<jobId>
        // Returns job information in JSON.
        // If the job isn't finished yet, then {"status": "RUNNING" | "ERROR"} is returned.
        // Returned JSON contains result attribute if status is "FINISHED"
        (get & path(Segment)) { jobId =>
          val timer = jobGetSingle.time()
          val statusFuture = jobInfoActor ? GetJobStatus(jobId)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            statusFuture.map {
              case NoSuchJobId =>
                notFound(ctx, "No such job ID " + jobId)
              case info: JobInfo =>
                val jobReport = getJobReport(info)
                val resultFuture = jobInfoActor ? GetJobResult(jobId)
                resultFuture.map {
                  case JobResult(_, result) =>
                    result match {
                      case s: Stream[_] =>
                        sendStreamingResponse(ctx, ResultChunkSize,
                          resultToByteIterator(jobReport, s.toIterator))
                      case _ => ctx.complete(jobReport ++ resultToTable(result))
                    }
                  case _ =>
                    ctx.complete(jobReport)
                }.recover {
                  case e: Exception =>
                    logAndComplete(ctx, "ERROR", 500, e)
                }
            }.recover {
              case e: Exception =>
                logAndComplete(ctx, "ERROR", 500, e)
            }.andThen {
              case _ => timer.stop()
            }
          }
        } ~
        //  DELETE /jobs/<jobId>
        //  Stop the current job. All other jobs submitted with this spark context
        //  will continue to run
        (delete & path(Segment)) { jobId =>
          val timer = jobDelete.time()
          val future = jobInfoActor ? GetJobStatus(jobId)
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            future.map {
              case NoSuchJobId =>
                notFound(ctx, "No such job ID " + jobId)
              case JobInfo(_, _, contextName, _, classPath, _, None, _, _) =>
                val jobManager = getJobManagerForContext(Some(contextName), config, classPath)
                val future = jobManager.get ? KillJob(jobId)
                future.map {
                  case JobKilled(_, _) => ctx.complete(Map(StatusKey -> JobStatus.Killed))
                }.recover {
                  case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
                }
              case JobInfo(_, _, _, _, state, _, _, Some(ex), _) if state.equals(JobStatus.Error) =>
                ctx.complete(Map(StatusKey -> JobStatus.Error, "ERROR" -> formatException(ex)))
              case JobInfo(_, _, _, _, state, _, _, _, _)
                if (state.equals(JobStatus.Finished) || state.equals(JobStatus.Killed)) =>
                notFound(ctx, "No running job with ID " + jobId)
              case _ => logAndComplete(ctx, "Received an unexpected message", 500)
            }.recover {
              case e: Exception =>
                logAndComplete(ctx, "ERROR", 500, e)
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
        get {
          parameters('limit.as[Int] ?, 'status.as[String] ?) { (limitOpt, statusOpt) =>
            val timer = jobGet.time()
            val limit = limitOpt.getOrElse(DefaultJobLimit)
            val statusUpperCaseOpt = statusOpt match {
              case Some(status) => Some(status.toUpperCase())
              case _ => None
            }
            val future = (jobInfoActor ? GetJobStatuses(Some(limit), statusUpperCaseOpt)).mapTo[Seq[JobInfo]]
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              future.map { infos =>
                val jobReport = infos.map { info =>
                  getJobReport(info)
                }
                ctx.complete(jobReport)
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
         * @return JSON result of { StatusKey -> "OK" | "ERROR", ResultKey -> "result"}, where "result" is
         *         either the job id, or a result
         */
        post {
          entity(as[String]) { configString =>
            parameters('appName ?, 'classPath ?, 'cp ?, 'mainClass ?,
              'context ?, 'sync.as[Boolean] ?, 'timeout.as[Int] ?, SparkJobUtils.SPARK_PROXY_USER_PARAM ?) {
              (appNameOpt, classPathOpt, cpOpt, mainClassOpt,
               contextOpt, syncOpt, timeoutOpt, sparkProxyUser) =>
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

                  import collection.JavaConverters._
                  val providedMainClass = Try(mainClassOpt.getOrElse(jobConfig.getString("mainClass"))).
                    getOrElse("")
                  val providedCp = if (cpOpt.isEmpty) {
                    Utils.getSeqFromConfig(jobConfig, "cp")
                  } else {
                    cpOpt.get.split(",").toSeq
                  }

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
                    binManagerFuture.map {
                      case BinaryInfoListForCp(binInfos) =>
                        logger.info(s"BinaryInfos from BinaryManager: $binInfos")
                        val jobManager = getJobManagerForContext(
                          contextOpt.map(_ => cName), cConfig, mainClass)
                        val events = if (async) asyncEvents else syncEvents
                        val future = jobManager.get.ask(
                          JobManagerActor.StartJob(
                            mainClass, binInfos, jobConfig, events))(timeout)
                        future.map {
                          case JobResult(jobId, res) =>
                            res match {
                              case s: Stream[_] => sendStreamingResponse(ctx, ResultChunkSize,
                                resultToByteIterator(Map.empty, s.toIterator))
                              case _ =>
                                ctx.complete(Map[String, Any]("jobId" -> jobId) ++ resultToTable(res))
                            }
                          case JobErroredOut(jobId, _, ex) =>
                            ctx.complete(Map[String, String]("jobId" -> jobId) ++ errMap(ex, "ERROR")
                            )
                          case JobStarted(_, jobInfo) =>
                            val future = jobInfoActor ? StoreJobConfig(jobInfo.jobId, postedJobConfig)
                            future.map {
                              case JobConfigStored =>
                                val jobReport = getJobReport(jobInfo, jobStarted = true)
                                ctx.complete(202, jobReport)
                            }.recover {
                              case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
                            }
                          case JobValidationFailed(_, _, ex) =>
                            logAndComplete(ctx, "VALIDATION FAILED", 400, ex)
                          case NoSuchFile(name) => notFound(ctx, "appName " + name + " not found")
                          case NoSuchClass => notFound(ctx, "classPath " + providedMainClass + " not found")
                          case WrongJobType => logAndComplete(ctx, "Invalid job type for this context", 400)
                          case JobLoadingError(err) => err match {
                            case _: MalformedURLException => logAndComplete(
                              ctx, "JOB LOADING FAILED: Malformed URL", StatusCodes.BadRequest.intValue, err)
                            case _ => logAndComplete (ctx, "JOB LOADING FAILED", 500, err)
                          }
                          case ContextStopInProgress =>
                            logAndComplete(ctx, "Context stop in progress", StatusCodes.Conflict)
                          case NoJobSlotsAvailable(maxJobSlots) =>
                            val errorMsg = "Too many running jobs (" + maxJobSlots.toString +
                              ") for job context '" + contextOpt.getOrElse("ad-hoc") + "'"
                            ctx.complete(503, Map(StatusKey -> "NO SLOTS AVAILABLE", ResultKey -> errorMsg))
                          case ContextInitError(e) => e match {
                            case _: MalformedURLException => logAndComplete(
                              ctx, "CONTEXT INIT ERROR: Malformed URL", StatusCodes.BadRequest.intValue, e)
                            case _ => logAndComplete(ctx, "CONTEXT INIT FAILED", 500, e)
                          }
                        }.recover {
                          case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
                        }.andThen {
                          case _ => timer.stop()
                        }
                      case NoSuchBinary(name) =>
                        notFound(ctx, "appName " + name + " not found")
                      case GetBinaryInfoListForCpFailure(ex) =>
                        logAndComplete(ctx, "ERROR", 500, ex)
                    }.recover {
                      case _: NoSuchElementException =>
                        ctx.complete(StatusCodes.NotFound, errMap("context " + contextOpt.get + " not found"))
                      case e: Exception => logAndComplete(ctx, "ERROR", 500, e)
                    }.andThen {
                      case _ => timer.stop()
                    }
                  }
                } catch {
                  case e @ (_: ConfigException | _: InsufficientConfiguration) =>
                    timer.stop()
                    complete(StatusCodes.BadRequest, errMap("Cannot parse config: " + e.getMessage))
                  case e: Exception =>
                    val stcode = 500
                    val errMes = "ERROR"
                    logger.info("StatusCode: " + stcode + ", ErrorMessage: " + errMes
                      + ", StackTrace: " + ErrorData.getStackTrace(e))
                    timer.stop()
                    complete(stcode, errMap(e, errMes))
                  }
              }
          }
        }
    }
  }

  override def timeoutRoute: Route =
    complete(500, errMap("Request timed out. Try using the /jobs/<jobID>, /jobs APIs to get status/results"))

  /**
   * if the shiro user is to be used as the proxy user, then this
   * computes the context name from the user name (and a prefix) and appends the user name
   * as the spark proxy user parameter to the config
   */
  def determineProxyUser(aConfig: Config,
                         authInfo: AuthInfo,
                         contextName: String): (String, Config) = {
    if (config.getBoolean("shiro.authentication") && config.getBoolean("shiro.use-as-proxy-user")) {
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
    val msg =
      if (context.isDefined) {
        GetContext(context.get)
      } else {
        StartAdHocContext(mainClass, contextConfig)
      }
    val future = (supervisor ? msg)(contextTimeout.seconds)
    Await.result(future, contextTimeout.seconds) match {
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
