package spark.jobserver.services

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

trait StaticService extends BaseService {
  def staticRoutes: Route = {
    path("") {
      getFromResource("html/index.html")
    } ~
    pathPrefix("html") {
      getFromResourceDirectory("html")
    }
  }
}
