package spark.jobserver.auth

import org.apache.shiro.realm.ldap.JndiLdapRealm
import org.apache.shiro.realm.ldap.LdapContextFactory
import org.apache.shiro.realm.ldap.JndiLdapContextFactory
import org.apache.shiro.realm.ldap.LdapUtils
import org.apache.shiro.subject.PrincipalCollection
import org.apache.shiro.authz._
import javax.naming.ldap.LdapContext
import javax.naming.directory._
import org.slf4j.LoggerFactory

/**
 * LDAP realm implementation that retrieves group information from LDAP and matches
 * the 'member' attribute values of each group against the given user.
 *
 * @note not all LDAP installations use the member property.... we might have to add
 *   memberOf matches as well and others
 *
 * @author KNIME (basics in Java stem from different sources by various authors from stackoverflow and such)
 */
class LdapGroupRealm extends JndiLdapRealm {
  import collection.JavaConverters._

  private val logger = LoggerFactory.getLogger(getClass)

  private val searchCtls: SearchControls = {
    val c = new SearchControls()
    c.setSearchScope(SearchControls.SUBTREE_SCOPE)
    c
  }

  lazy val searchBase: String = getContextFactory() match {
    case jni: JndiLdapContextFactory =>
      getEnvironmentParam(jni, "ldap.searchBase")
    case _ =>
      throw new RuntimeException("Configuration error: " +
        "LdapGroupRealm requires setting of the parameter 'searchBase'")
  }

  lazy val allowedGroups: Option[Array[String]] = getContextFactory() match {
    case jni: JndiLdapContextFactory =>
      val groups = getEnvironmentParam(jni, "ldap.allowedGroups").split(",").map(_.trim)
      if (groups.isEmpty) {
        None
      } else {
        logger.debug("Found allowedGroups: " + groups.mkString(", "))
        Some(groups)
      }
    case _ =>
      None
  }

  /** {0} = user name */
  private var userSearchFilter : String = "(&(objectClass=person)(CN={0}))"

  def setUserSearchFilter(aUserSearchFilter : String) {
    logger.debug("Setting user search filter to {}", aUserSearchFilter)
    userSearchFilter = aUserSearchFilter
  }

  def getUserSearchFilter : String = {
    userSearchFilter
  }

  /** {0} = allowed group, {1} = user name, {2} = user path */
  private var groupSearchFilter : String = "(&(member={2})(objectClass=posixGroup)(CN={0}))"

  def setGroupSearchFilter(aGroupSearchFilter : String) {
    logger.debug("Setting group search filter to {}", aGroupSearchFilter)
    groupSearchFilter = aGroupSearchFilter
  }

  def getGroupSearchFilter : String = {
    groupSearchFilter
  }

  private def getEnvironmentParam(jni: JndiLdapContextFactory, param: String): String = {
    val value = jni.getEnvironment().get(param)
    value match {
      case null =>
        //tell user what is missing instead of just throwing a NPE
        throw new RuntimeException("Configuration error: " +
          "LdapGroupRealm requires setting of the parameter '" + param + "'")
      case v =>
        v.toString
    }
  }

  override def queryForAuthorizationInfo(principals: PrincipalCollection,
                                         ldapContextFactory: LdapContextFactory): AuthorizationInfo = {

    val username = getAvailablePrincipal(principals).toString
    val ldapContext = ldapContextFactory.getSystemLdapContext()

    logger.debug("Running queryForAuthorizationInfo with principals: "
      + principals + ", user name: " + username)

    try {
      queryForAuthorizationInfo(ldapContext, username)
    } finally {
      LdapUtils.closeContext(ldapContext)
    }
  }


  def queryForAuthorizationInfo(ldapContext: LdapContext, username: String): AuthorizationInfo = {
    checkUser(ldapContext, username) match {
      case Some(userPath) =>
        getAllowedGroupsOrNoCheckOnGroups(ldapContext, username, userPath) match {
          case Some(groups) =>
            new SimpleAuthorizationInfo(groups.asJava)
          case None =>
            throw new AuthorizationException(LdapGroupRealm.ERROR_MSG_NO_VALID_GROUP)
        }
      case None =>
        throw new AuthorizationException(LdapGroupRealm.ERROR_MSG_AUTHORIZATION_FAILED)
    }
  }

  /** @returns full user path */
  def checkUser(ldapContext: LdapContext, username: String): Option[String] = {
    val searchAtts: Array[Object] = Array(username)

    logger.debug("checkingUser with search filter: " + userSearchFilter + ", user: " + username
      + " and search base: " + searchBase)

    val result = ldapContext.search(searchBase, userSearchFilter, searchAtts, searchCtls)
    if (result.hasMore) {
      val firstEntry = result.next
      try {
        Some(firstEntry.getNameInNamespace)
      } catch {
        case e: UnsupportedOperationException => Some(firstEntry.getName)
      }
    } else {
      None
    }
  }

  def getAllowedGroupsOrNoCheckOnGroups(ldapContext: LdapContext,
                                        username: String, userPath: String): Option[Set[String]] = {
    allowedGroups match {
      case Some(groups) =>
        val m = (groups map { group =>
          logger.debug("checking for group membership with filter: " + getGroupSearchFilter
            + ", group: " + group + ", user name: " + username + ", user path: " + userPath
            + " and search base: " + searchBase)

          val searchAtts: Array[Object] = Array(group, username, userPath)
          if (ldapContext.search(searchBase, groupSearchFilter, searchAtts, searchCtls).hasMore) {
            Some(group)
          } else {
            None
          }
        }).filter(_.isDefined).map(_.get)
        if (m.size > 0) {
          Some(m.toSet)
        } else {
          None
        }
      case None =>
        Some(Set())
    }
  }
}

object LdapGroupRealm {
  val ERROR_MSG_NO_VALID_GROUP = "no valid group found"
  val ERROR_MSG_AUTHORIZATION_FAILED = "user authorization failed"
}
