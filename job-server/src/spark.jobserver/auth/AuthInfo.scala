package spark.jobserver.auth

/**
 * based on example provided by Mario Camou
 * at
 * http://www.tecnoguru.com/blog/2014/07/07/implementing-http-basic-authentication-with-spray/
 */
class AuthInfo(val user: User) {
  def hasPermission(permission: String): Boolean = {
    // Code to verify whether user has the given permission
    true
  }
}