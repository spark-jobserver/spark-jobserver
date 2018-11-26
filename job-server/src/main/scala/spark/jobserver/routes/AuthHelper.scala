package spark.jobserver.routes
import akka.http.scaladsl.model.headers.HttpCredentials
import akka.http.scaladsl.server.Directives.AuthenticationResult
import spark.jobserver.auth.User

import scala.concurrent.Future

trait AuthHelper {

    type AuthMethod =
      Option[HttpCredentials] => Future[AuthenticationResult[User]]
}
