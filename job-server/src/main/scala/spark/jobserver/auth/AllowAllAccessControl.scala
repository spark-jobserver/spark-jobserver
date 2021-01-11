package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

/**
  * Default access control that accepts all users
  */
class AllowAllAccessControl(override protected val authConfig: Config)
                           (implicit ec: ExecutionContext, s: ActorSystem)
  extends SJSAccessControl(authConfig) {

  override def challenge(): SJSAccessControl.Challenge = {
    _ => Future.successful(authenticate(BasicHttpCredentials.apply("", "")))
  }

  override def authenticate(credentials: BasicHttpCredentials): Option[AuthInfo] =
    Some(new AuthInfo(User("anonymous")))

}
