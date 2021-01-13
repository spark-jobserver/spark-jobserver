package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import com.typesafe.config.Config
import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc._
import org.apache.shiro.authz.AuthorizationException
import org.apache.shiro.config.IniSecurityManagerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Apache Shiro based access control for the Spark JobServer, the authenticator realm must be
  * specified in the ini file for Shiro
  */
class ShiroAccessControl(override protected val authConfig: Config)
                        (implicit ec: ExecutionContext, s: ActorSystem)
  extends SJSAccessControl(authConfig) {

  val sManager = new IniSecurityManagerFactory(authConfig.getString("shiro.config.path")).getInstance()
  SecurityUtils.setSecurityManager(sManager)

  override def authenticate(credentials: BasicHttpCredentials): Option[AuthInfo] = {
    val BasicHttpCredentials(user, pass) = credentials
    val f = cache.getOrLoad(user, _ => {
      doAuthenticate(user, pass)
    })
      .map(Some(_))
      .recover {
        case uae: UnknownAccountException =>
          logger.info("ACCESS DENIED (Unknown), user [" + user + "]")
          cache.remove(user)
          None
        case ice: IncorrectCredentialsException =>
          logger.info("ACCESS DENIED (Incorrect credentials), user [" + user + "]")
          cache.remove(user)
          None
        case lae: LockedAccountException =>
          logger.info("ACCESS DENIED (Account is locked), user [" + user + "]")
          cache.remove(user)
          None
        case ae: AuthorizationException =>
          logger.info("ACCESS DENIED (" + ae.getMessage() + "), user [" + user + "]")
          cache.remove(user)
          None
        case ae: AuthenticationException =>
          logger.info("ACCESS DENIED (Authentication Exception), user [" + user + "]")
          cache.remove(user)
          None
      }
    Await.result(f, authTimeout.seconds)
  }

  private def doAuthenticate(user: String, pass: String): Future[AuthInfo] = {
    Future {
      val currentUser = SecurityUtils.getSubject()
      val token = new UsernamePasswordToken(user, pass)

      currentUser.login(token)
      val fullName = currentUser.getPrincipal().toString
      val permissions = Permissions.permissions
        .filter(p => currentUser.isPermitted(p.name) || currentUser.hasRole(p.name))
        .toSet
      currentUser.logout()
      createAuthInfo(fullName, permissions)
    }
  }

}
