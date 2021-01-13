package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.caching.LfuCache
import akka.http.caching.scaladsl.{Cache, CachingSettings}
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpChallenges}
import akka.http.scaladsl.server.Directive
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.AuthenticationResult
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import com.typesafe.config.{Config, ConfigException}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.Try

object SJSAccessControl {
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

class NoCaching[K, V] extends Cache[K, V] {
  override def apply(key: K, genValue: () => Future[V]): Future[V] = genValue()

  override def getOrLoad(key: K, loadValue: K => Future[V]): Future[V] = loadValue(key)

  override def get(key: K): Option[Future[V]] = None

  override def put(key: K, mayBeValue: Future[V])(implicit ex: ExecutionContext): Future[V] = mayBeValue

  override def remove(key: K): Unit = {}

  override def clear(): Unit = {}

  override def keys: Set[K] = Set.empty[K]

  override def size(): Int = 0
}


abstract class SJSAccessControl(protected val authConfig: Config)
                               (implicit ec: ExecutionContext, s: ActorSystem) {

  import scala.concurrent.duration._

  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  protected val authTimeout: Int = Try(authConfig.getDuration("auth-timeout",
    TimeUnit.MILLISECONDS).toInt / 1000).getOrElse(10)
  protected val useCaching: Boolean = authConfig.hasPath("use-cache") && authConfig.getBoolean("use-cache")

  protected val cache: Cache[String, AuthInfo] = if (useCaching) {
    Try(CachingSettings.apply(authConfig))
    .map(LfuCache.apply[String, AuthInfo])
    .recover {
      case _: ConfigException.Missing =>
        logger.warn("Failed to load access-control cache settings. Falling back to default cache settings.")
        LfuCache.apply[String, AuthInfo]
    }
    .get
  }
  else {
    logger.info("Disabled access control caching")
    new NoCaching()
  }

  def challenge(): SJSAccessControl.Challenge = {
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

  protected def createAuthInfo(name: String, permissions: Set[Permission]) = {
    logger.debug(f"Authenticated $name with roles $permissions")
    new AuthInfo(User(name), Option(permissions)
      .filter(_.nonEmpty)
      .getOrElse(Set(Permissions.ALLOW_ALL)))
  }

}

