package spark.jobserver.auth

import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

import scala.concurrent.Await

class SJSAuthenticatorSpec extends FunSpecLike
    with ScalatestRouteTest with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  //set this to true to check your real ldap server
  val isGroupChecking = false

  val config = ConfigFactory.parseString(s"""
    authentication-timeout = 10 s
    shiro.config.path = "${if (isGroupChecking) "classpath:auth/shiro.ini" else "classpath:auth/dummy.ini"}"
    """)

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

  val anonymousUser = "anonymous"
  val testUserInvalid = "no-user"
  val testUserInvalidPassword = "pw"

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  describe("AllowAllAuthenticator") {
    val instance = new AllowAllAuthenticator(config)

    it("should allow user with valid role/group") {
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(Some(new AuthInfo(User(anonymousUser))))
    }

    it("should allow user without valid role/group") {
      val cred = new BasicHttpCredentials(testUserWithoutValidGroup, testUserWithoutValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(Some(new AuthInfo(User(anonymousUser))))
    }

    it("should allow user without credentials") {
      Await.result(instance.challenge()(None), 10.seconds) should
        equal(Some(new AuthInfo(User(anonymousUser))))
    }
  }

  describe("ShiroAuthenticator") {
    val instance = new ShiroAuthenticator(config)

    it("should allow user with valid role/group") {
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(Some(new AuthInfo(User(testUserWithValidGroup))))
    }

    it("should check role/group when checking is activated") {
      val expected = if (isGroupChecking) {
        None
      } else {
        Some(new AuthInfo(User(testUserWithoutValidGroup)))
      }
      val cred = new BasicHttpCredentials(testUserWithoutValidGroup, testUserWithoutValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(expected)
    }

    it("should not allow invalid user") {
      val cred = new BasicHttpCredentials(testUserInvalid, testUserInvalidPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(None)
    }

    it("should not allow user without credentials") {
      val cred = new BasicHttpCredentials(testUserInvalid, testUserInvalidPassword)
      Await.result(instance.challenge()(None), 10.seconds) should
        equal(None)
    }
  }

}
