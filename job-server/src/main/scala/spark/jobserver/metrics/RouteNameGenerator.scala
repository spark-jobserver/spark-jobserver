package spark.jobserver.metrics

import akka.http.scaladsl.model.HttpRequest
import kamon.akka.http.DefaultNameGenerator

class RouteNameGenerator extends DefaultNameGenerator {
  override def generateTraceName(request: HttpRequest): String = {
    val path: String   = request.uri.path.toString
    val method: String = request.method.value
    s"$method-$path"
  }
}
