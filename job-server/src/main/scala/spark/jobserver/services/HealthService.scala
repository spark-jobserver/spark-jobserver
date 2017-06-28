package spark.jobserver.services

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._

trait HealthService extends BaseService {

  def healthRoute: Route = {
    pathPrefix("health"){
      pathEndOrSingleSlash {
        get {
          complete("OK")
        }
      }
    }
  }
}
