package spark.jobserver.auth

import Permissions._

/**
 * based on example provided by Mario Camou
 * at
 * http://www.tecnoguru.com/blog/2014/07/07/implementing-http-basic-authentication-with-spray/
 */
class AuthInfo(val user: User, val abilities: Set[Permission] = Set(ALLOW_ALL)) {
  def hasPermission(permission: Permission): Boolean = {
    abilities.contains(ALLOW_ALL) || // User can to everything
      abilities.contains(permission) || // User has required permission
      permission.parent.exists(abilities.contains) // User has parent of required permission
  }

  override def toString: String = user.login.toString

  override def equals(other: Any): Boolean =
    other match {
      case that: AuthInfo =>
        (that canEqual this) &&
          user == that.user
      case _ => false
    }

  def canEqual(other: Any): Boolean =
    other.isInstanceOf[AuthInfo]

  override def hashCode: Int = user.hashCode

}
