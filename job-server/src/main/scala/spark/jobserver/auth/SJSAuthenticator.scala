package spark.jobserver.auth

import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpChallenge, HttpCredentials}
import akka.http.scaladsl.server.directives.SecurityDirectives.{AuthenticationResult, Authenticator}
import akka.http.scaladsl.server.directives.{AuthenticationResult, Credentials}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc._
import org.apache.shiro.authz.AuthorizationException
import org.apache.shiro.util.Factory
import org.apache.shiro.subject.Subject

/**
 * Apache Shiro based authenticator for the Spark JobServer, the authenticator realm must be
 * specified in the ini file for Shiro
 */
trait SJSAuthenticator {
  def logger: Logger = LoggerFactory.getLogger(getClass)
  val challenge = HttpChallenge("SJSAuth", Some("SJS"))
  import scala.concurrent.duration._

  def asShiroAuthenticator(authTimeout: Int)(implicit ec: ExecutionContext): Option[HttpCredentials] => Future[AuthenticationResult[User]] = {
    credentials: Option[HttpCredentials] =>
    Future {
      credentials match {
      case Some(creds) if explicitValidation(creds, logger) => Right(User(creds.asInstanceOf[BasicHttpCredentials].username))
      case None => Left(challenge)
    }
    }
  }

 /**
   * do not call directly - only for unit testing!
   */
  def explicitValidation(userPass: HttpCredentials, logger: Logger)(implicit ec: ExecutionContext): Boolean = {

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
        true
      } catch {
        case uae: UnknownAccountException =>
          logger.info("ACCESS DENIED (Unknown), user [" + user + "]")
          false
        case ice: IncorrectCredentialsException =>
          logger.info("ACCESS DENIED (Incorrect credentials), user [" + user + "]")
          false
        case lae: LockedAccountException =>
          logger.info("ACCESS DENIED (Account is locked), user [" + user + "]")
          false
        case ae: AuthorizationException =>
          logger.info("ACCESS DENIED (" + ae.toString + "), user [" + user + "]")
          false
        case ae: AuthenticationException =>
          logger.info("ACCESS DENIED (Authentication Exception), user [" + user + "]")
          false
      }
    }

  /**
   * default authenticator that accepts all users
   * based on example provided by Mario Camou
   * at
   * http://www.tecnoguru.com/blog/2014/07/07/implementing-http-basic-authentication-with-spray/
   */
  def asAllUserAuthenticator(implicit ec: ExecutionContext): Option[HttpCredentials] => Future[AuthenticationResult[User]] = {
    credentials: Option[HttpCredentials] =>
      Future {
        credentials match {
          case _ => Right(User("anonymous"))
        }
      }
  }
}


