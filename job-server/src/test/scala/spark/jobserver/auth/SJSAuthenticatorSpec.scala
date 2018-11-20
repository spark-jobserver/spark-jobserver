package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, FunSpecLike, Matchers}
import org.apache.shiro.config.IniSecurityManagerFactory
import org.apache.shiro.mgt.DefaultSecurityManager
import org.apache.shiro.mgt.SecurityManager
import org.apache.shiro.realm.Realm
import org.apache.shiro.SecurityUtils
import org.apache.shiro.config.Ini
import spark.jobserver.auth._
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeoutException

import akka.http.scaladsl.model.headers.{BasicHttpCredentials, HttpCredentials}

object SJSAuthenticatorSpec {
    //edit this with your real LDAP server information, just remember not to
  // check it in....
  val LdapIniConfig = """
# use this for basic ldap authorization, without group checking
# activeDirectoryRealm = org.apache.shiro.realm.ldap.JndiLdapRealm
# use this for checking group membership of users based on the 'member' attribute of the groups:
activeDirectoryRealm = spark.jobserver.auth.LdapGroupRealm
# search base for ldap groups:
activeDirectoryRealm.contextFactory.environment[ldap.searchBase] = dc=xxx,dc=org
activeDirectoryRealm.contextFactory.environment[ldap.allowedGroups] = "cn=xx,ou=groups", "cn=spark,ou=groups",,,,,
activeDirectoryRealm.contextFactory.environment[java.naming.security.credentials] = password
activeDirectoryRealm.contextFactory.url = ldap://localhost:389
activeDirectoryRealm.userDnTemplate = cn={0},ou=people,dc=xxx,dc=org

cacheManager = org.apache.shiro.cache.MemoryConstrainedCacheManager

securityManager.cacheManager = $cacheManager
"""

  val DummyIniConfig = """
# =============================================================================
# Tutorial INI configuration
#
# Usernames/passwords are based on the classic Mel Brooks' film "Spaceballs" :)
# =============================================================================

# -----------------------------------------------------------------------------
# Users and their (optional) assigned roles
# username = password, role1, role2, ..., roleN
# -----------------------------------------------------------------------------
[users]
root = secret, admin
guest = guest, guest
presidentskroob = 12345, president
presidentskroob~2 = ludicrousspeed, darklord, schwartz
lonestarr = vespa, goodguy, schwartz

# -----------------------------------------------------------------------------
# Roles with assigned permissions
# roleName = perm1, perm2, ..., permN
# -----------------------------------------------------------------------------
[roles]
admin = *
schwartz = lightsaber:*
goodguy = winnebago:drive:eagle5
"""

}

class SJSAuthenticatorSpec extends SJSAuthenticator with FunSpecLike
    with ScalatestRouteTest with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  //set this to true to check your real ldap server
  val isGroupChecking = false
  val ini = {
    val tmp = new Ini()
    if (isGroupChecking) {
      tmp.load(SJSAuthenticatorSpec.LdapIniConfig)
    } else {
      tmp.load(SJSAuthenticatorSpec.DummyIniConfig)
    }
    tmp
  }
  val factory = new IniSecurityManagerFactory(ini)

  val sManager = factory.getInstance()
  SecurityUtils.setSecurityManager(sManager)

  override val logger = LoggerFactory.getLogger(getClass)

  val testUserWithValidGroup = if (isGroupChecking) {
    "user"
  } else {
    "lonestarr"
  }

  val testUserWithValidGroupPassword = if (isGroupChecking) {
    "user"
  } else {
    "vespa"
  }

  val testUserWithoutValidGroup = if (isGroupChecking) {
    "other-user"
  } else {
    "presidentskroob"
  }
  val testUserWithoutValidGroupPassword = if (isGroupChecking) {
    "other-password"
  } else {
    "12345"
  }

  val testUserInvalid = "no-user"
  val testUserInvalidPassword = "pw"

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  describe("SJSAuthenticator") {
    it("should allow user with valid role/group") {
      explicitValidation(new BasicHttpCredentials(testUserWithValidGroup,
        testUserWithValidGroupPassword), logger) should equal(true)
    }

    it("should check role/group when checking is activated") {
      val expected = if (isGroupChecking) {
        false
      } else {
        true
      }
      explicitValidation(new BasicHttpCredentials(testUserWithoutValidGroup,
        testUserWithoutValidGroupPassword), logger) should equal(expected)
    }

    it("should not allow invalid user") {
      explicitValidation(new BasicHttpCredentials(testUserInvalid,
        testUserInvalidPassword), logger) should equal(false)
    }
  }

}
