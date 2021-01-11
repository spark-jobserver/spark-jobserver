package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import com.typesafe.config.Config
import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc.{
  AuthenticationException, IncorrectCredentialsException,
  LockedAccountException, UnknownAccountException, UsernamePasswordToken
}
import org.apache.shiro.authz.AuthorizationException
import org.apache.shiro.config.IniSecurityManagerFactory

import scala.concurrent.ExecutionContext

/**
  * Apache Shiro based authenticator for the Spark JobServer, the authenticator realm must be
  * specified in the ini file for Shiro
  */
class ShiroAuthenticator(override protected val authConfig: Config)
                        (implicit ec: ExecutionContext, s: ActorSystem)
  extends SJSAuthenticator(authConfig) {

  val sManager = new IniSecurityManagerFactory(authConfig.getString("shiro.config.path")).getInstance()
  SecurityUtils.setSecurityManager(sManager)

  override def authenticate(credentials: BasicHttpCredentials): Option[AuthInfo] = {
    val currentUser = SecurityUtils.getSubject()
    val BasicHttpCredentials(user, pass) = credentials
    val token = new UsernamePasswordToken(user, pass)
    try {
      currentUser.login(token)
      val fullName = currentUser.getPrincipal().toString
      val permissions = Permissions.permissions
        .filter(p => currentUser.hasRole(p.name))
        .toSet
      logger.trace("ACCESS GRANTED, user [%s]", fullName)
      currentUser.logout()
      Option(new AuthInfo(User(fullName),
        Option(permissions)
          .filter(_.nonEmpty)
          .getOrElse(Set(Permissions.ALLOW_ALL)))
      )
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

}
