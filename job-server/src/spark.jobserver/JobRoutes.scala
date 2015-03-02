package spark.jobserver

import javax.ws.rs.Path

import akka.actor.{ActorRefFactory, ActorSystem, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.gettyimages.spray.swagger.SwaggerHttpService
import com.typesafe.config.{ Config, ConfigFactory, ConfigException, ConfigRenderOptions }
import java.util.NoSuchElementException
import com.wordnik.swagger.annotations._
import com.wordnik.swagger.model.ApiInfo
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
import spray.routing.{ HttpService, Route, RequestContext }
import scala.reflect.runtime.universe._

@Api(value = "/jobs", description = "Main routes for starting a job, listing existing jobs," +
  " getting job results")
trait JobRoutes extends HttpService with CommonRouteBehaviour {
  import CommonMessages._
  import scala.concurrent.duration._
  import ContextSupervisor._
  import ooyala.common.akka.web.JsonUtils._

  def config: Config
  def jobInfo: ActorRef
  implicit def ec: ExecutionContext =actorRefFactory.dispatcher

  val errorEvents: Set[Class[_]] = Set(classOf[JobErroredOut], classOf[JobValidationFailed])
  val asyncEvents = Set(classOf[JobStarted]) ++ errorEvents
  val syncEvents = Set(classOf[JobResult]) ++ errorEvents
  val supervisor: ActorRef

  val contextTimeout: Int

  import JobInfoActor._
  import JobManagerActor._
  
  // Note that the order is important since it defines matching order!
  def jobRoutes: Route = pathPrefix("jobs") { getConfig ~ getJobId ~ getJobs ~ postJobs}

  @Path("/{jobId}/config")
  @ApiOperation(httpMethod = "GET", response = classOf[String],
    value = "returns the configuration used to launch this job or an error if not found.")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "jobId", required = true, dataType = "String", paramType = "path",
      value = "Job Identifyer")
   ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "A json representation of ConfigObject configuration " +
      "used to launch this job "),
    new ApiResponse(code = 404, message = "If job for jobId was not found")))
  def getConfig: Route = (get & path(Segment / "config")) { jobId =>
      val renderOptions = ConfigRenderOptions.defaults().setComments(false).setOriginComments(false)

      val future = jobInfo ? GetJobConfig(jobId)
      respondWithMediaType(MediaTypes.`application/json`) { ctx =>
        future.map {
          case NoSuchJobId =>
            notFound(ctx, "No such job ID " + jobId.toString)
          case cnf: Config =>
            ctx.complete(cnf.root().render(renderOptions))
        }
      }
  }


  @ApiOperation(httpMethod = "GET", response = classOf[String],
    value = "returns the job's status.")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "jobId", required = true, dataType = "String", paramType = "path",
      value = "Job Identifyer")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message =
      """JSON result always starts with: {"status": "ERROR" / "OK" / "RUNNING"} If the job isn't finished yet,
         then {"status": "RUNNING" | "ERROR"} is returned.
      """),
    new ApiResponse(code = 404, message = "If job for jobId was not found")))
  def getJobId: Route = (get & path(Segment)) { jobId =>
        val future = jobInfo ? GetJobResult(jobId)
        respondWithMediaType(MediaTypes.`application/json`) { ctx =>
          future.map {
            case NoSuchJobId =>
              notFound(ctx, "No such job ID " + jobId.toString)
            case JobInfo(_, _, _, _, _, None, _) =>
              ctx.complete(Map(StatusKey -> "RUNNING"))
            case JobInfo(_, _, _, _, _, _, Some(ex)) =>
              ctx.complete(Map(StatusKey -> "ERROR", "ERROR" -> formatException(ex)))
            case JobResult(_, result) =>
              ctx.complete(resultToTable(result))
          }
        }
      }


  @ApiOperation(httpMethod = "GET", response = classOf[String],
    value = "returns a JSON list of hashes containing job status")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "limit", required = false, dataType = "String", paramType = "query",
      value = "limit to number of jobs to display, defaults to 50")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message =
      """A json array with each job and its satus, ie: {jobId: "word-count-2013-04-22", status: "RUNNING"}
      """),
    new ApiResponse(code = 404, message = "If job for jobId was not found")))
  def getJobs: Route = get {
        import JobInfoActor._
        import JobManagerActor._
    parameters('limit.as[Int] ?) { (limitOpt) =>
      val limit = limitOpt.getOrElse(DefaultJobLimit)
      val future = (jobInfo ? GetJobStatuses(Some(limit))).mapTo[Seq[JobInfo]]
      respondWithMediaType(MediaTypes.`application/json`) { ctx =>
        future.map { infos =>
          val jobReport = infos.map { info =>
            Map("jobId" -> info.jobId,
              "startTime" -> info.startTime.toString(),
              "classPath" -> info.classPath,
              "context"   -> (if (info.contextName.isEmpty) "<<ad-hoc>>" else info.contextName),
              "duration" -> getJobDurationString(info)) ++ (info match {
              case JobInfo(_, _, _, _, _, None, _)       => Map(StatusKey -> "RUNNING")
              case JobInfo(_, _, _, _, _, _, Some(ex))   => Map(StatusKey -> "ERROR",
                ResultKey -> formatException(ex))
              case JobInfo(_, _, _, _, _, Some(e), None) => Map(StatusKey -> "FINISHED")
            })
          }
          ctx.complete(jobReport)
        }
      }
    }
  }

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
  @ApiOperation(httpMethod = "POST", response = classOf[String],
    value = "Starts a new job.",
    notes =
      """The job JAR must have been previously uploaded, and
        the classpath must refer to an object that implements SparkJob.  The `validate()`
        API will be invoked before `runJob`.
      """)
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "appName", required = true, dataType = "String", paramType = "query",
      value = "the appName for the job JAR"),
    new ApiImplicitParam(name = "classPath", required = true, dataType = "String", paramType = "query",
      value = "the fully qualified class path for the job"),
    new ApiImplicitParam(name = "body", required = false, dataType = "String", paramType = "body",
      value = "The POST entity should be in Typesafe Config format. It will be merged with the " +
        "job server's config file at startup."),
    new ApiImplicitParam(name = "context", required = false, dataType = "String", paramType = "query",
      value = "the name of the context to run the job under.  If not specified, then a temporary " +
        "context is allocated for the job"),
    new ApiImplicitParam(name = "sync", required = false, dataType = "Boolean", paramType = "query",
      value = """if "true", then wait for and return results, otherwise return job Id"""),
    new ApiImplicitParam(name = "timeout", required = false, dataType = "Int", paramType = "query",
      value = "the number of seconds to wait for sync results to come back")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message =
      """JSON result of { StatusKey -> "OK" | "ERROR", ResultKey -> "result"}, where "result" is
         either the job id, or a result
      """),
    new ApiResponse(code = 202, message = "If the job just started"),
    new ApiResponse(code = 400, message = "If VALIDATION failed, invalid job for context or configuration" +
      " could not be parsed"),
    new ApiResponse(code = 404, message = "If the specified context could not be found"),
    new ApiResponse(code = 500, message = "If we could not load the job or unknown error"),
    new ApiResponse(code = 503, message = "If no slots are available (too many jobs running)")))
  def postJobs: Route = post {
    import JobInfoActor._
    import JobManagerActor._
    entity(as[String]) { configString =>
      parameters('appName, 'classPath,
        'context ?, 'sync.as[Boolean] ?, 'timeout.as[Int] ?) {
        (appName, classPath, contextOpt, syncOpt, timeoutOpt) =>
          try {
            val async = !syncOpt.getOrElse(false)
            val postedJobConfig = ConfigFactory.parseString(configString)
            val jobConfig = postedJobConfig.withFallback(config)
            val contextConfig = Try(jobConfig.getConfig("spark.context-settings")).
              getOrElse(ConfigFactory.empty)
            val jobManager = getJobManagerForContext(contextOpt, contextConfig, classPath)
            val events = if (async) asyncEvents else syncEvents
            val timeout = timeoutOpt.map(t => Timeout(t.seconds)).getOrElse(DefaultSyncTimeout)
            val future = jobManager.get.ask(
              JobManagerActor.StartJob(appName, classPath, jobConfig, events))(timeout)
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              future.map {
                case JobResult(_, res)       => ctx.complete(resultToTable(res))
                case JobErroredOut(_, _, ex) => ctx.complete(errMap(ex, "ERROR"))
                case JobStarted(jobId, context, _) =>
                  jobInfo ! StoreJobConfig(jobId, postedJobConfig)
                  ctx.complete(202, Map[String, Any](
                    StatusKey -> "STARTED",
                    ResultKey -> Map("jobId" -> jobId, "context" -> context)))
                case JobValidationFailed(_, _, ex) =>
                  ctx.complete(400, errMap(ex, "VALIDATION FAILED"))
                case NoSuchApplication => notFound(ctx, "appName " + appName + " not found")
                case NoSuchClass       => notFound(ctx, "classPath " + classPath + " not found")
                case WrongJobType      =>
                  ctx.complete(400, errMap("Invalid job type for this context"))
                case JobLoadingError(err) =>
                  ctx.complete(500, errMap(err, "JOB LOADING FAILED"))
                case NoJobSlotsAvailable(maxJobSlots) =>
                  val errorMsg = "Too many running jobs (" + maxJobSlots.toString +
                    ") for job context '" + contextOpt.getOrElse("ad-hoc") + "'"
                  ctx.complete(503, Map(StatusKey -> "NO SLOTS AVAILABLE", ResultKey -> errorMsg))
                case ContextInitError(e) => ctx.complete(500, errMap(e, "CONTEXT INIT FAILED"))
              }.recover {
                case e: Exception => ctx.complete(500, errMap(e, "ERROR"))
              }
            }
          } catch {
            case e: NoSuchElementException =>
              complete(StatusCodes.NotFound, errMap("context " + contextOpt.get + " not found"))
            case e: ConfigException =>
              complete(StatusCodes.BadRequest, errMap("Cannot parse config: " + e.getMessage))
            case e: Exception =>
              complete(500, errMap(e, "ERROR"))
          }
      }
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
        GetAdHocContext(classPath, contextConfig)
      }
    val future = (supervisor ? msg)(contextTimeout.seconds)
    Await.result(future, contextTimeout.seconds) match {
      case (manager: ActorRef, resultActor: ActorRef) => Some(manager)
      case NoSuchContext                              => None
      case ContextInitError(err)                      => throw new RuntimeException(err)
    }
  }
}
