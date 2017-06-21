package spark.jobserver.services

import scala.concurrent.ExecutionContextExecutor

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout
import scala.concurrent.duration._

trait BaseService {
  implicit val timeout = Timeout(60 seconds)
  implicit val system: ActorSystem
  implicit val executionContext: ExecutionContextExecutor
  implicit val materializer: Materializer

  val logger: LoggingAdapter

  def route: Route
}
