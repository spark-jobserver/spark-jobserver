package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

/**
  * default authenticator that accepts all users
  */
class AllowAllAuthenticator(override protected val config: Config)
                           (implicit ec: ExecutionContext, s: ActorSystem) extends SJSAuthenticator(config) {

  override def challenge(): SJSAuthenticator.Challenge = {
    _ => {
      Future {
        authenticate(BasicHttpCredentials.apply("", ""))
      }
    }
  }

  override def authenticate(credentials: BasicHttpCredentials): Option[AuthInfo] =
    Some(new AuthInfo(User("anonymous")))

}