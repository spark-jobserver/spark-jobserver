package spark.jobserver.auth

import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import io.jsonwebtoken._
import org.apache.shiro.codec.Base64
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import java.security.spec.X509EncodedKeySpec
import java.security.{Key, KeyFactory}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import Permissions._
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class SJSAccessControlSpec extends AnyFunSpecLike
    with ScalatestRouteTest with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  //set this to true to check your real ldap server
  val isGroupChecking = false

  private def config(cache: Boolean = true) = ConfigFactory.parseString(
    s"""
    auth-timeout = 10 s
    shiro.config.path = "${if (isGroupChecking) "classpath:auth/shiro.ini" else "classpath:auth/dummy.ini"}"
    keycloak {
      authServerUrl = "https://example.com/"
      realmName = master
      client = job-server
    }

    use-cache = ${cache}
    akka.http.caching.lfu-cache {
      max-capacity = 512
      initial-capacity = 16
      time-to-live = 1 hour
      time-to-idle = 10 minutes
    }
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

  describe("AllowAllAccessControl") {

    it("should allow user with valid role/group") {
      val instance = new AllowAllAccessControl(config())
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(Some(new AuthInfo(User(anonymousUser))))
    }

    it("should allow user without valid role/group") {
      val instance = new AllowAllAccessControl(config())
      val cred = new BasicHttpCredentials(testUserWithoutValidGroup, testUserWithoutValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(Some(new AuthInfo(User(anonymousUser))))
    }

    it("should allow user without credentials") {
      val instance = new AllowAllAccessControl(config())
      Await.result(instance.challenge()(None), 10.seconds) should
        equal(Some(new AuthInfo(User(anonymousUser))))
    }
  }

  describe("ShiroAccessControl") {
    it("should allow user with valid role/group") {
      val instance = new ShiroAccessControl(config())
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      val authInfo = Await.result(instance.challenge()(Some(cred)), 10.seconds)
      authInfo should equal(Some(new AuthInfo(User(testUserWithValidGroup))))
      authInfo.get.abilities should equal(Set(Permissions.ALLOW_ALL, Permissions.JOBS))
    }

    it("should check role/group when checking is activated") {
      val instance = new ShiroAccessControl(config())
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
      val instance = new ShiroAccessControl(config())
      val cred = new BasicHttpCredentials(testUserInvalid, testUserInvalidPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should
        equal(None)
    }

    it("should allow user with valid credentials after first rejection") {
      val instance = new ShiroAccessControl(config())
      val expected = if (isGroupChecking) {
        None
      } else {
        Some(new AuthInfo(User(testUserWithValidGroup)))
      }
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserInvalidPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(None)

      val cred2 = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred2)), 10.seconds) should equal(expected)
    }

    it("should allow cached user with invalid credentials") {
      val instance = new ShiroAccessControl(config())
      val expected = if (isGroupChecking) {
        None
      } else {
        Some(new AuthInfo(User(testUserWithValidGroup)))
      }

      val cred2 = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred2)), 10.seconds) should equal(expected)

      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserInvalidPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(expected)
    }

    it("should not allow cached user with invalid credentials if caching is disabled") {
      val instance = new ShiroAccessControl(config(false))
      val expected = if (isGroupChecking) {
        None
      } else {
        Some(new AuthInfo(User(testUserWithValidGroup)))
      }

      val cred2 = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred2)), 10.seconds) should equal(expected)

      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserInvalidPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(None)
    }

    it("should not allow user without credentials") {
      val instance = new ShiroAccessControl(config())
      Await.result(instance.challenge()(None), 10.seconds) should
        equal(None)
    }
  }

  describe("KeycloakAccessControl") {
    class MockedKeycloakAccessControl(override protected val authConfig: Config)
      extends KeycloakAccessControl(authConfig) {

      var loginCount = 0

      override protected def getJwtToken(username: String, password: String): Future[Jws[Claims]] = {
        Future {
          if (username == testUserWithValidGroup && password == testUserWithValidGroupPassword) {
            loginCount += 1
            Jwts.parser()
              .setSigningKeyResolver(new SigningKeyResolver {
                override def resolveSigningKey(header: JwsHeader[T] forSome {type T <: JwsHeader[T]},
                                               claims: Claims): Key =
                  resolveSigningKey(header, "")

                override def resolveSigningKey(header: JwsHeader[T] forSome {type T <: JwsHeader[T]},
                                               plaintext: String): Key = {
                  // Private RSA key only added for convenience if regeneration of JWT is necessary
                  val privateKey = {
//                    "-----BEGIN RSA PRIVATE KEY-----\n" +
                    "MIIEogIBAAKCAQEAwfU419m/GwODKNSgLeRF1peh6S/AlfrdW3RMzvM+OjqegE2o\n" +
                    "kDYBRmDq/6Tu3mgTwTdGMcJ+tJdYA3JRDmFiFPDKaLraAX9G9OlnZvCwfNInov+Q\n" +
                    "7hPChv4NgRTw+OdNMA2xQvpJSlYmlXqXVNDOX1rdiQD/jou6DCY5de95JhJxZgZ2\n" +
                    "ZlIl+rJHgkcQrV4LpylMWSBxEkNFmbgJ30lfU8PHJz2Eb6jpuu9LYWO4eUUZjd7C\n" +
                    "FjAwgQIXLD/eOLvRaT9IKVkxVfAlacikESbQGXttlHxWS+ZSjtMk54OU2AOOO3nj\n" +
                    "aWrkHVf1jBROKCo03+wq1Dl0t8gnpLeoIF/VIQIDAQABAoIBADNoJUrAgbBNPAQk\n" +
                    "ZtgC+qenxNgjOe4GcYj9yCXJvqJ8SupCqvyd87SNl3tuYYk9GI9LcSVbIW4H9uHi\n" +
                    "+KzRDsfyEhO0AngHHe1nt2pHPN+4a5z+E5GmVxakWzvtKvkthP3Jg0P3RlmXf956\n" +
                    "gYWPWkNXuAPJ6fIEAqmZr/0cHYYDQz+A3vUa4SevPLibI69zodKEB006ld+Wo0Oq\n" +
                    "mIKfJIwXBz3xdW2ZJ50YCTOPJkIXU7C5IjbZI+ZWlJvUbP84Vxe2Q2kv66/Uo1Yb\n" +
                    "sszT3uAgpluOK6QMsZusO6DOdFlTWuyYDsR95foRikX1IS3o1ahN1zWoCmaoSwiK\n" +
                    "Dh+LD+kCgYEA4G0lvonLMgdypOJ7TvFP6KkyenoL96qF7+gWYHUanlrIJHRGUsgE\n" +
                    "DL8+kUnBLNYxQACKxQ8GhZG7AknQPscC49v+bdeutQpF8kT9pR/kEIOJu934XCW9\n" +
                    "Uk/QudX/HYnrqm6yeW7GwdfWQraCU7mjyOhWXNLxCKVQGtWR0VgiR68CgYEA3T68\n" +
                    "A39bB5o5ljAGSsKjcaHS8zhGvoYuTdlHZ0CgXfG1Z2D7cqJUZ7GuVcL5cZdhsw43\n" +
                    "B27tnSXFm8pl0xdyYxJ+rVi4eUrsU7Qt0N0PSZfHVHKQuQDtSZPI+sdTKjpv5t2+\n" +
                    "I29Eru6WtcKCC1182TBVhkR/3amr0Iw4k3b1FC8CgYAXW6TTCPpqEZZgDOZyl/EO\n" +
                    "MRX841j9hPT9vDUgAvArTR2JlcR/9ytcvEbhzkBZz00+8Q+AZQjzu/Av08jlz8bA\n" +
                    "OnRnsEwRsakIByAzIHeXNGmQcRDZXmAvAfmibeBojaNGkNDojJwJLtKxDNfRqP+f\n" +
                    "+HaMoLPPh40nzdSoajjfJwKBgE1A7p26Bqss6xbKRigstq2i9+n9qJY2fEyqpggj\n" +
                    "xNuI3vLuJl7s19QtctZ3cmp7lZ3URNrPnSDWY532mn+PHF4Dwz/8Ts3rn4HK1ISt\n" +
                    "6/yihvOx3V78N98NP4xxtVR1e0V+ADqXS8BZhz6IYKhfSIz+F57+pDdeW6RCki7L\n" +
                    "xt/5AoGAblHDYcsO9GVAW8b0KWhczTgZaIEh8h7lJqFmvKIMLgQXOM0NFMwBqqQc\n" +
                    "KnzLZkJAPS9aTX4umjLXHjjEtmdt/iMmvViNPmkxU6QONT+xscO55glLETxDbaus\n" +
                    "PF3I7HF9JjevlRPFzGM4lhbVbw+GwsZNrcu4lu277qvlRLkjyls="
//                    "-----END RSA PRIVATE KEY-----"
                  }

                  val publicKey =
//                    "-----BEGIN PUBLIC KEY-----\n" +
                    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwfU419m/GwODKNSgLeRF\n" +
                      "1peh6S/AlfrdW3RMzvM+OjqegE2okDYBRmDq/6Tu3mgTwTdGMcJ+tJdYA3JRDmFi\n" +
                      "FPDKaLraAX9G9OlnZvCwfNInov+Q7hPChv4NgRTw+OdNMA2xQvpJSlYmlXqXVNDO\n" +
                      "X1rdiQD/jou6DCY5de95JhJxZgZ2ZlIl+rJHgkcQrV4LpylMWSBxEkNFmbgJ30lf\n" +
                      "U8PHJz2Eb6jpuu9LYWO4eUUZjd7CFjAwgQIXLD/eOLvRaT9IKVkxVfAlacikESbQ\n" +
                      "GXttlHxWS+ZSjtMk54OU2AOOO3njaWrkHVf1jBROKCo03+wq1Dl0t8gnpLeoIF/V\n" +
                      "IQIDAQAB"
//                      "-----END PUBLIC KEY-----"

                  val X509publicKey = new X509EncodedKeySpec(Base64.decode(publicKey))
                  val kf = KeyFactory.getInstance("RSA")
                  kf.generatePublic(X509publicKey)
                }
              })
              // JWT generated using https://jwt.io/ and private key provided above
              .parseClaimsJws("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjBkYkNLbk9PT3FoekRtSXJP" +
                "c2lEeS02em5rQWVDamhCYkVfUVVjam5nd1EifQ.eyJleHAiOjMwMDAwMDAwMDAsImlhdCI6MTYwODI5NDk" +
                "yMCwianRpIjoiMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAiLCJpc3MiOiJodHRwczovL" +
                "2V4YW1wbGUuY29tL2F1dGgvcmVhbG1zL3Rlc3QiLCJhdWQiOlsic3Bhcmstam9ic2VydmVyLWF1dGgiLCJ" +
                "hY2NvdW50Il0sInN1YiI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwIiwidHlwIjoiQ" +
                "mVhcmVyIiwiYXpwIjoidGVzdCIsInNlc3Npb25fc3RhdGUiOiIwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0" +
                "wMDAwMDAwMDAwMCIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cHM6Ly9leGFtcGxlLmNvb" +
                "SIsImh0dHA6Ly9leGFtcGxlLmNvbSJdLCJyZWFsbV9hY2Nlc3MiOnt9LCJyZXNvdXJjZV9hY2Nlc3MiOns" +
                "iam9iLXNlcnZlciI6eyJyb2xlcyI6WyIqIiwiY29udGV4dHM6cmVhZCJdfSwiYWNjb3VudCI6eyJyb2xlc" +
                "yI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSw" +
                "ic2NvcGUiOiJlbWFpbCBwcm9maWxlIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsIm5hbWUiOiJ1c2VyIG5hb" +
                "WUiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJsb25lc3RhcnIiLCJnaXZlbl9uYW1lIjoidXNlciIsImZhbWl" +
                "seV9uYW1lIjoibmFtZSIsImVtYWlsIjoidXNlci5uYW1lQGV4YW1wbGUuY29tIn0.WBl0UV3IWHfrPOVtX" +
                "nQ3--paFN1NCER5VyEPaxkqkXFBAT9C3yzCth0xvCljqw1NB8MUfkaGir_clSBYGnRVL-VtzZK4mMCoIei" +
                "gC1DJXAbPE2EjpWHBimoFnilkl1PTp-FV3Cg82bTdY4fGi-VdK9phghIcb64QMdI42rzvGFq9vHgtzNnyo" +
                "Hj6u6RNotHJjCrijxNoPHk0V-SxZVmDjMFfmgJ4OKd4eIsLLUEHBUKK-WMp6R-Preu4bPjyCkFhugMdVcj" +
                "MIdqyiniF08mOrTfCp4MPxmJD6_ty8y9SFs6dZCk7VvjgpIQO2ysrPk6Aqhd_xorr0ySfPEc0-Gy5fg")
          }
          else {
            throw new IncorrectCredentialsException
          }
        }
      }
    }

    val validAuthInfo = Some(new AuthInfo(User(testUserWithValidGroup)))

    it("should allow user with valid credentials") {
      val instance = new MockedKeycloakAccessControl(config())
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(validAuthInfo)
    }

    it("should cache user with valid credentials") {
      val instance = new MockedKeycloakAccessControl(config())
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(validAuthInfo)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(validAuthInfo)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(validAuthInfo)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(validAuthInfo)

      assert(instance.loginCount == 1)
    }

    it("should allow user with valid credentials after first rejection") {
      val instance = new MockedKeycloakAccessControl(config())
      val cred = new BasicHttpCredentials(testUserWithValidGroup, testUserInvalidPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(None)

      val cred2 = new BasicHttpCredentials(testUserWithValidGroup, testUserWithValidGroupPassword)
      Await.result(instance.challenge()(Some(cred2)), 10.seconds) should equal(validAuthInfo)
    }

    it("should not allow invalid user") {
      val instance = new MockedKeycloakAccessControl(config())
      val cred = new BasicHttpCredentials(testUserInvalid, testUserInvalidPassword)
      Await.result(instance.challenge()(Some(cred)), 10.seconds) should equal(None)
    }

    it("should not allow user without credentials") {
      val instance = new MockedKeycloakAccessControl(config())
      Await.result(instance.challenge()(None), 10.seconds) should equal(None)
    }
  }

  describe("AuthInfo") {

    it("should allow user with ALLOW_ALL") {
      val authInfo = new AuthInfo(User(testUserWithValidGroup), Set(Permissions.ALLOW_ALL))

      assert(authInfo.hasPermission(ALLOW_ALL))
      assert(authInfo.hasPermission(BINARIES))
      assert(authInfo.hasPermission(CONTEXTS_READ))
      assert(authInfo.hasPermission(DATA_RESET))
    }

    it("should allow user with group permission") {
      val authInfo = new AuthInfo(User(testUserWithValidGroup), Set(Permissions.BINARIES))

      assert(authInfo.hasPermission(BINARIES))
      assert(authInfo.hasPermission(BINARIES_DELETE))
      assert(authInfo.hasPermission(BINARIES_UPLOAD))
      assert(authInfo.hasPermission(BINARIES_READ))

      assert(!authInfo.hasPermission(ALLOW_ALL))
      assert(!authInfo.hasPermission(DATA))
      assert(!authInfo.hasPermission(JOBS_DELETE))
    }

    it("should allow user with single permission") {
      val authInfo = new AuthInfo(User(testUserWithValidGroup),
        Set(Permissions.BINARIES_READ, Permissions.CONTEXTS_READ))

      assert(authInfo.hasPermission(BINARIES_READ))
      assert(authInfo.hasPermission(CONTEXTS_READ))

      assert(!authInfo.hasPermission(ALLOW_ALL))
      assert(!authInfo.hasPermission(BINARIES))
      assert(!authInfo.hasPermission(BINARIES_DELETE))
      assert(!authInfo.hasPermission(DATA))
      assert(!authInfo.hasPermission(JOBS_DELETE))
    }

    it("should not allow user without permissions") {
      val authInfo = new AuthInfo(User(testUserWithValidGroup), Set.empty)

      assert(!authInfo.hasPermission(ALLOW_ALL))
      assert(!authInfo.hasPermission(BINARIES))
      assert(!authInfo.hasPermission(BINARIES_DELETE))
      assert(!authInfo.hasPermission(DATA))
      assert(!authInfo.hasPermission(JOBS_DELETE))
    }

  }

}
