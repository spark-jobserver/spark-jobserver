package spark.jobserver.services

import scala.concurrent.ExecutionContextExecutor

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.Config

class JobServerServices(config: Config,
                        port: Int,
                        binaryManager: ActorRef,
                        dataManager: ActorRef,
                        supervisor: ActorRef,
                        val jobActor: ActorRef)(implicit val system: ActorSystem) extends JobService {

  override val logger = Logging(system, getClass)

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val materializer: Materializer                 = ActorMaterializer()

  def start() = {
    println(s"Starting up on port $port")
    Http().bindAndHandle(route, "0.0.0.0", port)
  }

  override def route: Route = jobRoutes
}
