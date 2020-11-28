package spark.jobserver.common.akka.web

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import javax.net.ssl.SSLContext

import scala.concurrent.ExecutionContextExecutor

/**
 * Contains methods for starting an embedded Spray web server.
 */
object WebService {
  /**
   * Starts a web server given a Route.  Note that this call is meant to be made from an App or other top
   * level scope, and not within an actor, as system.actorOf may block.
   *
   * @param route The spray Route for the service.  Multiple routes can be combined like (route1 ~ route2).
   * @param system the ActorSystem to use
   * @param host The host string to bind to, defaults to "0.0.0.0"
   * @param port The port number to bind to
   */
  def start(route: Route, system: ActorSystem, host: String = "0.0.0.0",
            port: Int = 8080, https: Boolean = false)(implicit sslContext: SSLContext) {
    implicit val actorSystem: ActorSystem = system
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    if (https) {
      Http()(system)
        .newServerAt(host, port)
        .enableHttps(ConnectionContext.httpsServer(sslContext))
        .bind(route)
    }
    else {
      Http()(system)
        .newServerAt(host, port)
        .bind(route)
    }
  }
}
