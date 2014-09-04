package spark.jobserver

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.NotificationActor.JobNotification
import spray.client.pipelining._
import scala.Some
import scala.concurrent.Future
import spray.http.{HttpRequest, Uri, HttpResponse}
import scala.util.{Failure, Success}
import scala.concurrent.duration._


object NotificationActor {

  //Request
  case class JobNotification(jobId: String, status: String, callbackUrlOpt: Option[String])

  // Akka 2.2.x style actor props for actor creation
  def props(): Props = Props(classOf[NotificationActor])
}

class NotificationActor extends InstrumentedActor {

  implicit val system: ActorSystem = ActorSystem()
  implicit val timeout: Timeout = Timeout(3.seconds)
  import system.dispatcher // implicit execution context
  val pipeline: HttpRequest => Future[HttpResponse] = sendReceive


  def wrappedReceive: Receive = {
    case JobNotification(jobId, status, Some(callbackUrl))=>{
      logger.info("Notify about " + jobId + " with status " + status + " to uri :" + callbackUrl)
      val uri = Uri(callbackUrl + "?jobId=" + jobId + "&status=" + status)
      val responseFuture: Future[HttpResponse] = pipeline(Get(uri))
      responseFuture onComplete {
        case Success(httpResponse) =>
          logger.info("Posted to the " + uri + " status: " + httpResponse.status)
          logger.info("content: " + httpResponse.entity.asString)
        case Failure(error) =>
          val msg = s"Failed to call the callback uri  $callbackUrl for $jobId with status $status"
          logger.error( msg )
      }
    }
    case JobNotification(jobId, status, None)=>{
      logger.info("Notification about " + jobId + " with status " + status)
    }
  }
}
