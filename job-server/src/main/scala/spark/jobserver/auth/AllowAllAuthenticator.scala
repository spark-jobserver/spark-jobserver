package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

/**
  * Default authenticator that accepts all users
  */
class AllowAllAuthenticator(override protected val authConfig: Config)
                           (implicit ec: ExecutionContext, s: ActorSystem)
  extends SJSAuthenticator(authConfig) {

  override def challenge(): SJSAuthenticator.Challenge = {
    _ => Future.successful(authenticate(BasicHttpCredentials.apply("", "")))
  }

  override def authenticate(credentials: BasicHttpCredentials): Option[AuthInfo] =
    Some(new AuthInfo(User("anonymous")))

}
