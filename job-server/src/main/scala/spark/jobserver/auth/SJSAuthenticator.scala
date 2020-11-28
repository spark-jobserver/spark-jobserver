package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpChallenges, HttpCredentials}
import akka.http.scaladsl.server.Directive
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.AuthenticationResult
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc._
import org.apache.shiro.authz.AuthorizationException
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future, TimeoutException}

/**
 * Apache Shiro based authenticator for the Spark JobServer, the authenticator realm must be
 * specified in the ini file for Shiro
 */
trait SJSAuthenticator {

  import scala.concurrent.duration._

  def logger: Logger = LoggerFactory.getLogger(getClass)

  type Challenge = Option[BasicHttpCredentials] => Future[Option[AuthInfo]]

  protected val realm = "Sparkjobserver Private"

  def customAuthenticateBasicAsync(authenticator: Challenge): Directive[Tuple1[AuthInfo]] = {
     extractExecutionContext.flatMap { implicit ec =>
        authenticateOrRejectWithChallenge[BasicHttpCredentials, AuthInfo] { cred =>
          authenticator(cred).fast.map {
            case Some(t) => AuthenticationResult.success(t)
            case None => {
              AuthenticationResult.failWithChallenge(HttpChallenges.basic(realm))
            }
          }
        }
      }
  }

  def asShiroAuthenticator(authTimeout: Int)
                          (implicit ec: ExecutionContext, s: ActorSystem): Challenge = {
    credentials: Option[BasicHttpCredentials] => {
      lazy val f = Future {
        credentials match {
          case Some(p) if explicitValidation(p, logger).isDefined =>
            Some(new AuthInfo(User(p.username)))
          case _ => None
        }
      }
      import akka.pattern.after
      lazy val t = after(duration = authTimeout second,
        using = s.scheduler)(Future.failed(new TimeoutException("Authentication timed out!")))

      Future firstCompletedOf Seq(f, t)
    }
  }

  /**
   * do not call directly - only for unit testing!
   */
  def explicitValidation(userPass: HttpCredentials, logger: Logger): Option[AuthInfo] = {
    val currentUser = SecurityUtils.getSubject()
    val BasicHttpCredentials(user, pass) = userPass
    val token = new UsernamePasswordToken(user, pass)
    try {
      currentUser.login(token)
      val fullName = currentUser.getPrincipal().toString
      //is this user allowed to do anything -
      //  realm implementation may for example throw an exception
      //  if user is not a member of a valid group
      currentUser.isPermitted("*")
      logger.trace("ACCESS GRANTED, user [%s]", fullName)
      currentUser.logout()
      Option(new AuthInfo(new User(fullName)))
    } catch {
      case uae: UnknownAccountException =>
        logger.info("ACCESS DENIED (Unknown), user [" + user + "]")
        None
      case ice: IncorrectCredentialsException =>
        logger.info("ACCESS DENIED (Incorrect credentials), user [" + user + "]")
        None
      case lae: LockedAccountException =>
        logger.info("ACCESS DENIED (Account is locked), user [" + user + "]")
        None
      case ae: AuthorizationException =>
        logger.info("ACCESS DENIED (" + ae.getMessage() + "), user [" + user + "]")
        None
      case ae: AuthenticationException =>
        logger.info("ACCESS DENIED (Authentication Exception), user [" + user + "]")
        None
    }
  }

  /**
   * default authenticator that accepts all users
   */
  def asAllUserAuthenticator(implicit ec: ExecutionContext): Challenge = {
    credentials: Option[BasicHttpCredentials] =>
      Future {
        credentials match {
          case _ => Some(new AuthInfo(User("anonymous")))
        }
      }
  }
}


