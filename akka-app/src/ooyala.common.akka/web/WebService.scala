package ooyala.common.akka.web

import akka.actor.ActorSystem
import com.typesafe.config.Config
import spray.can.server.ServerSettings
import spray.routing.{Route, SimpleRoutingApp}

/**
 * Contains methods for starting an embedded Spray web server.
 */
object WebService extends SimpleRoutingApp {
  /**
   * Starts a web server given a Route.  Note that this call is meant to be made from an App or other top
   * level scope, and not within an actor, as system.actorOf may block.
   *
   * @param route The spray Route for the service.  Multiple routes can be combined like (route1 ~ route2).
   * @param system the ActorSystem to use
   * @param host The host string to bind to, defaults to "0.0.0.0"
   * @param port The port number to bind to
   */
  def start(route: Route, system: ActorSystem, config: Config,
            host: String = "0.0.0.0", port: Int = 8080) {
    implicit val actorSystem = system
    startServer(host, port, settings = Some(ServerSettings.fromSubConfig(config)))(route)
  }
}
