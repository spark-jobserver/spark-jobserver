package spark.jobserver.auth

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import spray.routing.directives.AuthMagnet
import spray.routing.authentication.UserPass
import spray.routing.authentication.BasicAuth

import spray.routing.authentication._
import spray.routing.directives.AuthMagnet

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }
import org.slf4j.LoggerFactory
import com.typesafe.config.Config
import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc._
import org.apache.shiro.config.IniSecurityManagerFactory
import org.apache.shiro.mgt.SecurityManager
import org.apache.shiro.util.Factory
import org.apache.shiro.subject.Subject

trait SJSAuthenticator {

  def asShiroAuthenticator(config: Config)(implicit ec: ExecutionContext): AuthMagnet[AuthInfo] = {
    val logger = LoggerFactory.getLogger(getClass)

    //TODO - can we read this information from the config instead?
    val ldapFactory = new IniSecurityManagerFactory(config.getString("shiro.path.ini"))
    val sManager = ldapFactory.getInstance()
    SecurityUtils.setSecurityManager(sManager)

    val allowedGroups: Option[java.util.List[String]] = {
      if (config.hasPath("shiro.ldap.allowedGroups")) {
        val g = config.getStringList("shiro.ldap.allowedGroups")
        if (g.size() > 0) { Some(g) } else { None }
      } else {
        None
      }
    }

    def isInAllowedGroupOrNoCheckOnGroups(currentUser: Subject): Boolean = {
      allowedGroups match {
        case Some(groups) => {
          val roles: Array[Boolean] = currentUser.hasRoles(groups)
          roles.foldLeft(false)((r, isInGroup) => isInGroup || r)
        }
        case None => {
          true
        }
      }
    }

    def validate(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
      val currentUser = SecurityUtils.getSubject()

      //if (!currentUser.isAuthenticated()) {
      Future {
        val UserPass(user, pass) = userPass.get
        val token = new UsernamePasswordToken(user, pass)
        try {
          currentUser.login(token)
          val fullName = currentUser.getPrincipal().toString
          if (isInAllowedGroupOrNoCheckOnGroups(currentUser)) {
            logger.trace("ACCESS GRANTED for user [" + fullName + "]")
            currentUser.logout()
            Option(new AuthInfo(new User(fullName)))
          } else {
            currentUser.logout()
            logger.info("ACCESS DENIED (GROUP)")
            None
          }
        } catch {
          case uae: UnknownAccountException => {
            logger.info("Unknown user")
            None
          }
          case ice: IncorrectCredentialsException => {
            logger.info("Incorrect credentials")
            None
          }
          case lae: LockedAccountException => {
            logger.info("Account is Locked")
            None
          }
          case ae: AuthenticationException => {
            logger.info("Authentication Exception")
            None
          }
        }
      }
    }

    def authenticator(userPass: Option[UserPass]): Future[Option[AuthInfo]] = Future {
      Await.result(validate(userPass), Duration.Inf)
    }

    BasicAuth(authenticator _, realm = "LDAP Private")
  }

  /**
   * default authenticator that accepts all users
   * based on example provided by Mario Camou
   * at
   * http://www.tecnoguru.com/blog/2014/07/07/implementing-http-basic-authentication-with-spray/
   */
  def asAllUserAuthenticator(implicit ec: ExecutionContext): AuthMagnet[AuthInfo] = {
    def validateUser(userPass: Option[UserPass]): Option[AuthInfo] = {
      Some(new AuthInfo(new User("anonymous")))
    }

    def authenticator(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
      Future { validateUser(userPass) }
    }

    BasicAuth(authenticator _, realm = "Private API")
  }
}


