package spark.jobserver.routes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import spark.jobserver.common.akka.web.JsonUtils._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshalling.ToResponseMarshallable._

import akka.actor.ActorRef
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.model.HttpEntity.{ChunkStreamPart, Chunked}
import akka.http.scaladsl.model.{ContentTypes, HttpResponse, ResponseEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.scaladsl
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.CommonMessages._
import spark.jobserver.ContextSupervisor._
import spark.jobserver.WebApiUtils._
import spark.jobserver._
import spark.jobserver.auth.AuthInfo
import spark.jobserver.common.akka.web.JsonUtils._
import spark.jobserver.io.{JobInfo, JobStatus}
import spark.jobserver.util.SparkJobUtils
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Main routes for starting a job, listing existing jobs, getting job results
  */
trait JobRoutes extends AuthHelper with ConfigHelper {

  def jobInfo: ActorRef
  def supervisor: ActorRef

  protected def asyncEvents: Set[Class[_]]
  protected def syncEvents: Set[Class[_]]

  def jobRoutes(authenticator: AuthMethod, config: Config,
                contextTimeout: Int)
               (implicit t: Timeout): Route = pathPrefix("jobs") {
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

        onComplete(jobInfo ? GetJobConfig(jobId)) {
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
          onComplete(jobInfo ? GetJobStatus(jobId)) {
            case Success(value) =>
              value match {
                case NoSuchJobId =>
                  complete(
                    StatusCodes.NotFound,
                    errMap(s"No such job ID $jobId")
                  )
                case info: JobInfo =>
                  val jobReport = getJobReport(info)
                  onComplete(jobInfo ? GetJobResult(jobId)) {
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
          onComplete(jobInfo ? GetJobStatus(jobId)) {
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
                    classPath,
                    contextTimeout
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
                    Map(StatusKey -> JobStatus.Error, "ERROR" -> WebApiUtils.formatException(ex))
                  )
                case JobInfo(_, _, _, _, _, state, _, _, _)
                  if state.equals(JobStatus.Finished) || state
                    .equals(JobStatus.Killed) =>
                  complete(StatusCodes.NotFound, errMap(s"No such job ID $jobId"))
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
                (jobInfo ? GetJobStatuses(Some(limit), statusUpperCaseOpt))
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
              parameters('appName, 'classPath, 'context ?, 'sync.as[Boolean] ?, 'timeout.as[Int] ?,
                SparkJobUtils.SPARK_PROXY_USER_PARAM ?
              ) {
                (appName, classPath, contextOpt, syncOpt, timeoutOpt, sparkProxyUser) =>
                  val async = !syncOpt.getOrElse(false)
                  val postedJobConfig = ConfigFactory.parseString(configString)
                  val jobConfig = postedJobConfig.withFallback(config).resolve()
                  val contextConfig = getContextConfig(
                    jobConfig, sparkProxyUser
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
                    classPath,
                    contextTimeout
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
                              this.jobInfo ? StoreJobConfig(
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
                            complete(StatusCodes.InternalServerError, errMap(e, "CONTEXT INIT FAILED"))
                        }
                      case Failure(ex) =>
                        ex match {
                          case e: Exception =>
                            complete(StatusCodes.InternalServerError, errMap(e, "ERROR"))
                        }
                    }
                  }
              }
            }
          }
        }
    }
  }

  private def getJobManagerForContext(context: Option[String], contextConfig: Config,
                                      classPath: String, contextTimeout: Int): Option[ActorRef] = {
    import ContextSupervisor._
    val msg =
      if (context.isDefined) {
        GetContext(context.get)
      } else {
        StartAdHocContext(classPath, contextConfig)
      }
    val future = (supervisor ? msg)(contextTimeout.seconds)
    Await.result(future, contextTimeout.seconds) match {
      case manager: ActorRef => Some(manager)
      case NoSuchContext => None
      case ContextInitError(err) => throw new RuntimeException(err)
    }
  }
}
