package spark.jobserver.auth

import org.apache.shiro.realm.ldap.JndiLdapRealm
import org.apache.shiro.realm.ldap.LdapContextFactory
import org.apache.shiro.realm.ldap.LdapUtils
import org.apache.shiro.subject.PrincipalCollection
import org.apache.shiro.authz._
import javax.naming.ldap.LdapContext
import javax.naming.directory._
import javax.naming.NamingEnumeration
import org.slf4j.LoggerFactory
import com.typesafe.config.{ Config, ConfigFactory }

/**
 * LDAP realm implementation that retrieves group information from LDAP and matches
 * the 'member' attribute values of each group against the given user.
 *
 * @note not all LDAP installations use the member property.... we might have to add
 *   memberOf matches as well and others
 *
 * @author dwk (basics in Java stem from different sources by various authors from stackoverflow and such)
 */
class LdapGroupRealm extends JndiLdapRealm {

  private val logger = LoggerFactory.getLogger(getClass)

  private val searchFilter = "(&(objectClass=person)(CN={0}))"

  //TODO - this property is set in JobServer.scala (after it is retrieved from
  //  the merged configuration) -- is there a better way?
  private val searchBase: String = System.getProperty("shiro.ldap.searchBase")

  private val searchCtls: SearchControls = {
    val c = new SearchControls();
    c.setSearchScope(SearchControls.SUBTREE_SCOPE);
    c
  }

  override def queryForAuthorizationInfo(principals: PrincipalCollection,
                                         ldapContextFactory: LdapContextFactory): AuthorizationInfo = {

    val username = getAvailablePrincipal(principals).toString

    // Perform context search
    val ldapContext = ldapContextFactory.getSystemLdapContext();

    try {
      val roleNames: java.util.Set[String] = getRoleNamesForUser(username, ldapContext);
      buildAuthorizationInfo(roleNames);
    } finally {
      LdapUtils.closeContext(ldapContext);
    }
  }

  def buildAuthorizationInfo(roleNames: java.util.Set[String]): AuthorizationInfo = {
    return new SimpleAuthorizationInfo(roleNames);
  }

  //TODO - can (should?) this information be cached?
  private def retrieveGroups(ldapContext: LdapContext): Map[String, Set[String]] = {
    val groupMemberFilter = "(member=*)"
    logger.trace("Retrieving group memberships.")

    val groupSearchAtts: Array[Object] = Array()
    val groupAnswer = ldapContext.search(searchBase, groupMemberFilter,
      groupSearchAtts, searchCtls);

    val members = scala.collection.mutable.Map[String, Set[String]]()

    while (groupAnswer.hasMoreElements()) {
      val sr2: SearchResult = groupAnswer.next()

      if (logger.isDebugEnabled()) {
        logger.debug("Checking members of group [" + sr2.getName() + "]")
      }

      members += sr2.getName() -> getMembers(sr2)
    }
    members.toMap
  }

  private def getMembers(sr: SearchResult): Set[String] = {
    val attrs: Attributes = sr.getAttributes();

    val members = scala.collection.mutable.HashSet[String]()
    if (attrs != null) {
      val ae: NamingEnumeration[_ <: Attribute] = attrs.getAll();
      while (ae.hasMore()) {
        val attr: Attribute = ae.next();

        if (attr.getID().equals("member")) {
          val javaMembers: java.util.Collection[String] = LdapUtils.getAllAttributeValues(attr)

          val iter: java.util.Iterator[String] = javaMembers.iterator()
          while (iter.hasNext()) {
            members += iter.next()
          }
        }
      }
    }
    members.toSet
  }

  private def getRoleNamesForUser(username: String, ldapContext: LdapContext): java.util.Set[String] = {
    val roleNames: java.util.Set[String] = new java.util.LinkedHashSet[String]()

    val searchAtts: Array[Object] = Array(username)

    val answer: NamingEnumeration[SearchResult] = ldapContext.search(searchBase, searchFilter,
      searchAtts, searchCtls);

    val members = retrieveGroups(ldapContext)

    while (answer.hasMoreElements()) {
      val userSearchResult: SearchResult = answer.next()

      val fullGroupMemberName = "%s,%s" format (userSearchResult.getName(), searchBase)
      val groups: Set[String] = members.filter(entry => {
        entry._2.contains(fullGroupMemberName)
      }).map(e => e._1).toSet

      groups.foreach(g => roleNames.add(g))
    }
    roleNames
  }
}
