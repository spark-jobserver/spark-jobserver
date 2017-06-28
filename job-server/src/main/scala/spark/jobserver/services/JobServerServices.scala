package spark.jobserver.services

import scala.concurrent.ExecutionContextExecutor

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config

case class JobServerServices(cfg: Config, port: Int, bm: ActorRef, dm: ActorRef, supervisor: ActorRef, jobActor: ActorRef)
                            (implicit val sys: ActorSystem) extends JobService with HealthService with StaticService {

  override val logger = Logging(sys, getClass)

  implicit val executionContext: ExecutionContextExecutor = sys.dispatcher
  implicit val materializer: Materializer                 = ActorMaterializer()

  def start(): Unit = {
    logger.info(s"Starting up on port $port")
    Http().bindAndHandle(route, "0.0.0.0", port)
  }

  def route: Route = jobRoutes ~ healthRoute ~ staticRoutes
}
