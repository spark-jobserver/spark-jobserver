package spark.jobserver.auth

import akka.actor.{ Actor, Props }
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import spark.jobserver._
import spark.jobserver.util.SparkJobUtils
import spark.jobserver.io.{BinaryType, BinaryInfo, JobInfo}
import org.joda.time.DateTime
import org.scalatest.{ Matchers, FunSpec, BeforeAndAfterAll }
import spray.http.StatusCodes._
import spray.http.HttpHeaders.Authorization
import spray.http.BasicHttpCredentials
import spray.routing.{ HttpService, Route }
import spray.routing.directives.AuthMagnet
import spray.testkit.ScalatestRouteTest
import org.apache.shiro.config.IniSecurityManagerFactory
import org.apache.shiro.mgt.{ DefaultSecurityManager, SecurityManager }
import org.apache.shiro.realm.Realm
import org.apache.shiro.SecurityUtils
import org.apache.shiro.config.Ini
import scala.collection.mutable.SynchronizedSet
import scala.util.Try
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import spray.routing.directives.AuthMagnet
import spray.routing.authentication.UserPass
import spray.routing.authentication.BasicAuth
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeoutException

// Tests authorization only, actual responses are tested elsewhere
// Does NOT test underlying Supervisor / JarManager functionality
// HttpService trait is needed for the sealRoute() which wraps exception handling
class WebApiWithAuthenticationSpec extends FunSpec with Matchers with BeforeAndAfterAll
    with ScalatestRouteTest with HttpService {
  import scala.collection.JavaConverters._
  import spray.httpx.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._
  import spark.jobserver.common.akka.web.JsonUtils._

  def actorRefFactory = system

  val bindConfKey = "spark.jobserver.bind-address"
  val bindConfVal = "127.0.0.1"
  val masterConfKey = "spark.master"
  val masterConfVal = "spark://localhost:7077"
  val config = ConfigFactory.parseString(s"""
    spark {
      master = "${masterConfVal}"
      jobserver.bind-address = "${bindConfVal}"
      jobserver.short-timeout = 3 s
    }
    shiro {
      authentication = on
    }
                                 """)

  val dummyPort = 9999

  // See http://doc.akka.io/docs/akka/2.2.4/scala/actors.html#Deprecated_Variants;
  // for actors declared as inner classes we need to pass this as first arg
  private val dummyActor = system.actorOf(Props(classOf[DummyActor], this))

  private def routesWithTimeout(useAsProxyUser: Boolean, authTimeout: Int): Route = {
    val testConfig = config.withValue("shiro.authentication-timeout",
      ConfigValueFactory.fromAnyRef(authTimeout)).withValue("shiro.use-as-proxy-user", ConfigValueFactory.fromAnyRef(useAsProxyUser))
    val api = new WebApi(system, testConfig, dummyPort, dummyActor, dummyActor, dummyActor, dummyActor) {
      private def asShiroAuthenticatorWithWait(authTimeout: Int)(implicit ec: ExecutionContext): AuthMagnet[AuthInfo] = {
        val logger = LoggerFactory.getLogger(getClass)

        def validate(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
          //if (!currentUser.isAuthenticated()) {
          Future {
            explicitValidation(userPass getOrElse UserPass("", ""), logger)
          }
        }

        def authenticator(userPass: Option[UserPass]): Future[Option[AuthInfo]] = Future {
          if (authTimeout > 0) {
            Await.result(validate(userPass), authTimeout.seconds)
          } else {
            throw new TimeoutException("forced timeout")
          }
        }

        BasicAuth(authenticator _, realm = "Shiro Private")
      }
      
      override def initSecurityManager() {
        val ini = {
          val tmp = new Ini()
          tmp.load(SJSAuthenticatorSpec.DummyIniConfig)
          tmp
        }
        val factory = new IniSecurityManagerFactory(ini)

        val sManager = factory.getInstance()
        SecurityUtils.setSecurityManager(sManager)
      }

      override lazy val authenticator: AuthMagnet[AuthInfo] = {
        logger.info("Using authentication.")
        initSecurityManager()
        asShiroAuthenticatorWithWait(authTimeout)
      }
    }
    api.myRoutes
  }

  private val routesWithProxyUser = routesWithTimeout(true, 1000)
  private val routesWithoutProxyUser = routesWithTimeout(false, 1000)

  private val USER_NAME = "presidentskroob"
  private val USER_NAME_2 = USER_NAME + WebApi.NameContextDelimiter + "2"

  // set to some valid user
  private val authorization = new Authorization(new BasicHttpCredentials(USER_NAME, "12345"))
  private val authorizationInvalidPassword = new Authorization(new BasicHttpCredentials(USER_NAME, "xxx"))
  private val authorizationUnknownUser = new Authorization(new BasicHttpCredentials("whoami", "xxx"))
  private val dt = DateTime.parse("2013-05-29T00Z")
  private val jobInfo =
    JobInfo("foo-1", "context", BinaryInfo("demo", BinaryType.Jar, dt), "com.abc.meme", dt, None, None)
  private val ResultKey = "result"

  private val addedContexts = new scala.collection.mutable.HashSet[String] with SynchronizedSet[String]

  class DummyActor extends Actor {
    import CommonMessages._
    import JobInfoActor._
    def receive = {
      case ListBinaries(_)                => sender ! Map()
      case GetJobStatus(id)               => sender ! jobInfo
      case GetJobResult(id)               => sender ! JobResult(id, id + "!!!")
      case ContextSupervisor.ListContexts => sender ! addedContexts.toSeq
      case ContextSupervisor.AddContext(name, _) =>
        if (addedContexts.contains(name)) {
          sender ! ContextSupervisor.ContextAlreadyExists
        } else {
          addedContexts.add(name)
          sender ! ContextSupervisor.ContextInitialized
        }
      case ContextSupervisor.StopContext(name) =>
        addedContexts.remove(name)
        sender ! ContextSupervisor.ContextStopped
      case ContextSupervisor.AddContextsFromConfig =>
        sender ! "OK"
      case m =>
        //dev support when adding new test cases:
        sys.error("Unhandled message: " + m)
        sender ! m.toString
    }
  }

  describe("jars routes") {
    it("should allow user with valid authorization") {
      Get("/jars").withHeaders(authorization) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(OK)
      }
    }

    it("should not allow user with invalid password") {
      Post("/jars/foobar", Array[Byte](0, 1, 2)).
        withHeaders(authorizationInvalidPassword) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }

    it("should not allow unknown user") {
      Get("/jars").withHeaders(authorizationUnknownUser) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }
  }

  describe("binaries routes") {

    it("should allow user with valid authorization") {
        Get("/binaries").withHeaders(authorization) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be (OK)
      }
    }

    it("should not allow user with invalid password") {
      Post("/binaries/pyfoo", Array[Byte](0, 1, 2)).
        withHeaders(authorizationInvalidPassword, BinaryType.Egg.contentType) ~>
        sealRoute(routesWithProxyUser) ~> check {
        status should be (Unauthorized)
      }
    }

    it("should not allow unknown user") {
      Post("/binaries/pyfoo", Array[Byte](0, 1, 2)).
        withHeaders(authorizationUnknownUser, BinaryType.Egg.contentType) ~>
        sealRoute(routesWithProxyUser) ~> check {
        status should be (Unauthorized)
      }
    }
  }

  describe("/jobs routes") {
    it("should allow user with valid authorization") {
      Get("/jobs/foobar").withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }
    }

    it("should not allow user with invalid password") {
      val config2 = "foo.baz = booboo"
      Post("/jobs?appName=foo&classPath=com.abc.meme&context=one&sync=true", config2).withHeaders(authorizationInvalidPassword) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(Unauthorized)
        }
    }

    it("should not allow unknown user") {
      Delete("/jobs/job_to_kill").withHeaders(authorizationUnknownUser) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }
  }

  describe("context routes") {
    it("should allow user with valid authorization") {
      Get("/contexts").withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }
    }

    it("should only show visible contexts to user") {
      val testRoutes = routesWithProxyUser
      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      addedContexts.clear
      //the parameter SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME should have no effect
      Post("/contexts/one?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME).withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }
      Post("/contexts/two").withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }
      //again, proxy user param should have no effect
      Post("/contexts/three?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XXX").withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }
      while (!addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + "one") && !addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + "two") &&
        !addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + "three")) {
        Thread.sleep(3)
      }

      //this user should see both contexts
      Get("/contexts/").withHeaders(authorization) ~> sealRoute(testRoutes) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("one", "two", "three"))
      }

      //another user should not see any contexts
      Get("/contexts/").withHeaders(authorization2) ~> sealRoute(testRoutes) ~> check {
        status should be(OK)
        responseAs[Seq[String]] should be(Seq())
      }
      //clear
      addedContexts.clear
    }

    it("should allow user name prefixes in contexts") {
      val testRoutes = routesWithProxyUser
      //clear
      addedContexts.clear
      //add a context that has the user name as a prefix
      Post("/contexts/" + USER_NAME + "_c").withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }
      //add same context without the user name prefix
      Post("/contexts/" + "c").withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }
      //add an impersonating contexts that already has the user name as a prefix
      Post("/contexts/" + USER_NAME + "_c2?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME).withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }

      while (!addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + "c") &&
        !addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + USER_NAME + "_c") &&
        !addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + USER_NAME + "_c2")) {
        Thread.sleep(3)
      }

      Get("/contexts/").withHeaders(authorization) ~> sealRoute(testRoutes) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("c", USER_NAME + "_c", USER_NAME + "_c2"))
      }
      addedContexts.clear
    }

    it("should allow user names that contain prefixes") {
      val testRoutes = routesWithProxyUser
      //clear
      addedContexts.clear
      //add some context as USER_NAME
      Post("/contexts/c1").withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }
      //add another context as USER_NAME_2
      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      Post("/contexts/c2").withHeaders(authorization2) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }

      while (!addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + "c1") &&
        !addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + WebApi.NameContextDelimiter + "2" + WebApi.NameContextDelimiter + "c2")) {
        Thread.sleep(3)
      }

      Get("/contexts/").withHeaders(authorization) ~> sealRoute(testRoutes) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("c1"))
      }
      Get("/contexts/").withHeaders(authorization2) ~> sealRoute(testRoutes) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("c2"))
      }
      addedContexts.clear
    }

    //test what happens when user uses the proxy user param context with the same name?
    it("should ignore proxy user param") {
      val cName = "Z"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME).withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }
      Thread.sleep(5)
      addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + cName) should equal(true)
      Post("/contexts/" + cName).withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(BadRequest)
        }
      //another proxy user should also not work
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XYZ").withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(BadRequest)
        }
      addedContexts.clear
    }

    it("should allow contexts of the same name by different users") {
      val cName = "X"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME).withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }
      Thread.sleep(5)
      Get("/contexts/").withHeaders(authorization) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set(cName))
      }

      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      Post("/contexts/" + cName).withHeaders(authorization2) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }

      Thread.sleep(1)
      Get("/contexts/").withHeaders(authorization) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set(cName))
      }
      Get("/contexts/").withHeaders(authorization2) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set(cName))
      }
      addedContexts.clear
    }

    it("should not allow user with invalid password") {
      Post("/contexts/one").withHeaders(authorizationInvalidPassword) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(Unauthorized)
        }
    }

    it("should allow user with valid authorization to impersonate herself") {
      Post("/contexts/validImpersonation?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME).withHeaders(authorization) ~>
        sealRoute(routesWithoutProxyUser) ~> check {
          status should be(OK)
        }
      Delete("/contexts/validImpersonation").withHeaders(authorization) ~> sealRoute(routesWithoutProxyUser) ~> check {
        status should be(OK)
      }
    }

    it("should allow user with valid authorization to impersonate another user when flag is off") {
      val cName = "who"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XYZ").withHeaders(authorization) ~>
        sealRoute(routesWithoutProxyUser) ~> check {
          status should be(OK)
        }
      //any user should be able to see this context
      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      Get("/contexts/").withHeaders(authorization2) ~> sealRoute(routesWithoutProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]].contains(cName) should be(true)
      }
      Get("/contexts/").withHeaders(authorization) ~> sealRoute(routesWithoutProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]].contains(cName) should be(true)
      }
    }

    it("should not allow unknown user") {
      Delete("/contexts/xxx").withHeaders(authorizationUnknownUser) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }

    it("should allow valid user to delete context with proper impersonation") {
      addedContexts.clear
      Post("/contexts/xxx?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME).withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }

      while (!addedContexts.contains(USER_NAME + WebApi.NameContextDelimiter + "xxx")) {
        Thread.sleep(3)
      }
      Get("/contexts/").withHeaders(authorization) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("xxx"))
      }
      Delete("/contexts/xxx").withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }
      Get("/contexts/").withHeaders(authorization) ~> sealRoute(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set())
      }
    }
  }

  describe("routes with timeout") {
    it("jobs should not allow user with valid authorization when timeout") {
      Get("/jobs/foobar").withHeaders(authorization) ~>
        sealRoute(routesWithTimeout(true, 0)) ~> check {
          status should be(InternalServerError)
        }
    }

    it("jars should not allow user with valid authorization when timeout") {
      Get("/jars").withHeaders(authorization) ~>
        sealRoute(routesWithTimeout(false, 0)) ~> check {
          status should be(InternalServerError)
        }
    }

    it("contexts should not allow user with valid authorization when timeout") {
      Get("/contexts").withHeaders(authorization) ~>
        sealRoute(routesWithTimeout(true, 0)) ~> check {
          status should be(InternalServerError)
        }
    }
  }
}

