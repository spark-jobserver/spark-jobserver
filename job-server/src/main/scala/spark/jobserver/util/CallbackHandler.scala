package spark.jobserver.util

import akka.actor.{ActorSystem, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri
import org.slf4j.LoggerFactory
import spark.jobserver.common.akka.web.JsonUtils._
import spark.jobserver.io.JobInfo
import spark.jobserver.util.ResultMarshalling.{exceptionToMap, resultToTable}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success => TrySuccess}

trait CallbackHandler {

  def success(info: JobInfo, result: Any)(implicit system: ActorSystem): Unit

  def failure(info: JobInfo, error: Throwable)(implicit system: ActorSystem): Unit
}


object StandaloneCallbackHandler extends CallbackHandler with SprayJsonSupport {

  private val logger = LoggerFactory.getLogger(getClass)

  override def success(info: JobInfo, result: Any)(implicit system: ActorSystem): Unit = {
    info.callbackUrl
      .foreach(url => {
        val resMap = result match {
          case _: Stream[_] =>
            // Stream results can have arbitrary size and can not be send via POST.
            // Substitute with fixed token to indicate stream result
            resultToTable("<Stream result>", Some(info.jobId))
          case _ => resultToTable(result, Some(info.jobId))
        }
        doRequest(info.jobId, info.callbackUrl.get, resMap)
      })
  }

  override def failure(info: JobInfo, error: Throwable)(implicit system: ActorSystem): Unit = {
    info.callbackUrl
      .foreach(url => {
        val resMap = exceptionToMap(info.jobId, error)
        doRequest(info.jobId, info.callbackUrl.get, resMap)
      })
  }

  private def doRequest(jobId: String, callbackUrl: Uri, content: Map[String, Any])
                       (implicit system: ActorSystem): Unit = {
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    implicit val scheduler: Scheduler = system.scheduler

    akka.pattern.retry(() => {
      logger.debug(s"Processing callback for $jobId")
      Http()
        .singleRequest(Post(callbackUrl, content))
        .map(res => {
          if (res.status.intValue() >= 400) {
            throw new IllegalStateException(s"Request failed with ${res.status}")
          }
          res
        })
        .recover {
          case NonFatal(ex) =>
            logger.debug(s"Callback for $jobId failed with ${ex.getMessage}. Trying again.")
            throw ex
        }
    }, attempts = 5, 10 seconds)
      .onComplete {
        case Failure(ex) => logger.warn(s"Callback for $jobId failed with ${ex.getMessage}")
        case TrySuccess(res) => logger.info(s"Callback completed with ${res.status}");
      }
  }
}