package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpChallenges}
import akka.http.scaladsl.server.Directive
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.AuthenticationResult
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.Try

object SJSAuthenticator {
  type Challenge = Option[BasicHttpCredentials] => Future[Option[AuthInfo]]

  protected val realm = "Sparkjobserver Private"

  def customAuthenticateBasicAsync(authenticator: Challenge): Directive[Tuple1[AuthInfo]] = {
    extractExecutionContext.flatMap { implicit ec =>
      authenticateOrRejectWithChallenge[BasicHttpCredentials, AuthInfo] { cred =>
        authenticator(cred).fast.map {
          case Some(t) => AuthenticationResult.success(t)
          case None =>
            AuthenticationResult.failWithChallenge(HttpChallenges.basic(realm))
        }
      }
    }
  }
}

abstract class SJSAuthenticator(protected val authConfig: Config)
                      (implicit ec: ExecutionContext, s: ActorSystem) {
  import scala.concurrent.duration._

  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  protected val authTimeout: Int = Try(authConfig.getDuration("authentication-timeout",
    TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(10)

  def challenge(): SJSAuthenticator.Challenge = {
    credentials: Option[BasicHttpCredentials] => {
      lazy val f = Future {
        credentials match {
          case Some(p) =>
            authenticate(p) match {
              case Some(authInfo: AuthInfo) =>
                logger.debug(f"Authenticated ${authInfo.user} with permissions ${authInfo.abilities}")
                Some(authInfo)
              case None =>
                logger.debug(f"Failed to authenticate ${p.username}")
                None
            }
          case _ =>
            logger.debug("No credentials provided")
            None
        }
      }
      import akka.pattern.after
      lazy val t = after(duration = authTimeout.seconds,
        using = s.scheduler)(Future.failed(new TimeoutException("Authentication timed out!")))

      Future firstCompletedOf Seq(f, t)
    }
  }

  protected def authenticate(credentials: BasicHttpCredentials): Option[AuthInfo]

}

