package spark.jobserver.services

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout

trait BaseService {
  implicit val timeout: Timeout = Timeout(60 seconds)

  implicit val sys: ActorSystem
  implicit val executionContext: ExecutionContextExecutor
  implicit val materializer: Materializer

  val logger: LoggingAdapter

  def route: Route

}
