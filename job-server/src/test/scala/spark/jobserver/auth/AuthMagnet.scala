package spark.jobserver.auth
import akka.http.scaladsl.server.AuthenticationFailedRejection.CredentialsMissing
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Directive1}
import akka.http.scaladsl.server.directives.AuthenticationDirective
import akka.http.scaladsl.server.directives.BasicDirectives._
import akka.http.scaladsl.server.directives.FutureDirectives._
import akka.http.scaladsl.server.directives.AuthenticationDirective._
import akka.http.scaladsl.server.directives.RouteDirectives._

import scala.concurrent.{ExecutionContext, Future}

class AuthMagnet[T](authDirective: Directive1[AuthenticationDirective[T]])(implicit executor: ExecutionContext) {

  val directive: Directive1[T] = authDirective.flatMap {
    case Right(user)     => provide(user)
    case Left(rejection) => reject(rejection)
  }

  val optionalDirective: Directive1[Option[T]] = authDirective.flatMap {
    case Right(user)                                                => provide(Some(user))
    case Left(AuthenticationFailedRejection(CredentialsMissing, _)) => provide(None)
    case Left(rejection)                                            => reject
  }
}

object AuthMagnet {
  implicit def fromFutureAuth[T](auth: => Future[AuthenticationDirective[T]])(implicit executor: ExecutionContext): AuthMagnet[T] =
    new AuthMagnet(onSuccess(auth))

  implicit def fromContextAuthenticator[T](auth: ContextAuthenticator[T])(implicit executor: ExecutionContext): AuthMagnet[T] =
    new AuthMagnet(extract(auth).flatMap(onSuccess(_)))
}
