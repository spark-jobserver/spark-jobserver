package spark.jobserver


import akka.actor.{ ActorSystem, ActorRef }
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory, ConfigException, ConfigRenderOptions }
import java.util.NoSuchElementException
import ooyala.common.akka.web.{ WebService, CommonRoutes }
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.util.SparkJobUtils
import spray.routing.RequestContext
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try
import spark.jobserver.io.JobInfo
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._


trait CommonRouteBehaviour extends CommonRoutes {
  import scala.concurrent.duration._

  def config: Config

  implicit def ec: ExecutionContext = actorRefFactory.dispatcher

  implicit val ShortTimeout = Timeout(3 seconds)
  val DefaultSyncTimeout = Timeout(10 seconds)
  val DefaultJobLimit = 50
  val StatusKey = "status"
  val ResultKey = "result"

  private[jobserver] def badRequest(ctx: RequestContext, msg: String) =
    ctx.complete(StatusCodes.BadRequest, errMap(msg))

  private[jobserver] def notFound(ctx: RequestContext, msg: String) =
    ctx.complete(StatusCodes.NotFound, errMap(msg))

  private[jobserver] def errMap(errMsg: String) = Map(StatusKey -> "ERROR", ResultKey -> errMsg)

  private[jobserver] def errMap(t: Throwable, status: String) =
    Map(StatusKey -> status, ResultKey -> formatException(t))

  private[jobserver] def getJobDurationString(info: JobInfo): String =
    info.jobLengthMillis.map { ms => ms / 1000.0 + " secs" }.getOrElse("Job not done yet")

  def resultToMap(result: Any): Map[String, Any] = result match {
    case m: Map[_, _] => m.map { case (k, v) => (k.toString, v) }.toMap
    case s: Seq[_]    => s.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case a: Array[_]  => a.toSeq.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case item         => Map(ResultKey -> item)
  }

  def resultToTable(result: Any): Map[String, Any] = {
    Map(StatusKey -> "OK", ResultKey -> result)
  }

  def formatException(t: Throwable): Any =
    if (t.getCause != null) {
      Map("message" -> t.getMessage,
        "errorClass" -> t.getClass.getName,
        "cause" ->   t.getCause.getMessage,
        "causingClass" -> t.getCause.getClass.getName,
        "stack" -> t.getCause.getStackTrace.map(_.toString).toSeq)
    } else {
      Map("message" -> t.getMessage,
        "errorClass" -> t.getClass.getName,
        "stack" -> t.getStackTrace.map(_.toString).toSeq)
    }


}
