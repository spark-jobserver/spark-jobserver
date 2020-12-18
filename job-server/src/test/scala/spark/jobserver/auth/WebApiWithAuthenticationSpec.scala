package spark.jobserver.auth

import java.util.concurrent.TimeoutException
import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import spark.jobserver._
import spark.jobserver.auth.SJSAuthenticator.Challenge
import spark.jobserver.io.JobDAOActor.GetJobInfo
import spark.jobserver.io.{BinaryInfo, BinaryType, JobInfo, JobStatus}
import spark.jobserver.util.SparkJobUtils

import scala.collection.mutable.SynchronizedSet
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

// Tests authorization only, actual responses are tested elsewhere
// Does NOT test underlying Supervisor / JarManager functionality
// HttpService trait is needed for the seal() which wraps exception handling
class WebApiWithAuthenticationSpec extends FunSpec with Matchers with BeforeAndAfterAll
    with ScalatestRouteTest with SprayJsonSupport{
  import spray.json._
  import DefaultJsonProtocol._

  def actorRefFactory: ActorSystem = system

  val bindConfKey = "spark.jobserver.bind-address"
  val bindConfVal = "127.0.0.1"
  val masterConfKey = "spark.master"
  val masterConfVal = "spark://localhost:7079"
  val config = ConfigFactory.parseString(s"""
    spark {
      master = "${masterConfVal}"
      jobserver.bind-address = "${bindConfVal}"
      jobserver.short-timeout = 3 s
    }
    authentication {
      authentication-timeout = 10 s
      shiro.config.path = "classpath:auth/dummy.ini"
    }
                                 """)

  val dummyPort = 9997

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  // See http://doc.akka.io/docs/akka/2.2.4/scala/actors.html#Deprecated_Variants;
  // for actors declared as inner classes we need to pass this as first arg
  private val dummyActor = system.actorOf(Props(classOf[DummyActor], this))

  private def routesWithTimeout(useAsProxyUser: Boolean, authTimeout: Int): Route = {
    val testConfig = config.withValue("authentication.authentication-timeout",
      ConfigValueFactory.fromAnyRef(authTimeout))
      .withValue("authentication.shiro.use-as-proxy-user", ConfigValueFactory.fromAnyRef(useAsProxyUser))
    val api = new WebApi(system, testConfig, dummyPort, dummyActor,
      dummyActor, dummyActor, dummyActor, null) {

      class MockedShiroAuthenticator(override protected val authConfig: Config)
                                    (implicit ec: ExecutionContext, s: ActorSystem)
        extends ShiroAuthenticator(authConfig)(ec, s){

        override def challenge(): Challenge = {
          credentials: Option[BasicHttpCredentials] => {
            Future {
              val userPass: BasicHttpCredentials = credentials match {
                case Some(p) => p
                case _ => BasicHttpCredentials("", "")
              }
              if (authTimeout > 0) {
                val authInfo = authenticate(userPass) match {
                  case Some(p) => Some(new AuthInfo(User(userPass.username)))
                  case None => None
                }
                cache.remove(userPass.username)
                authInfo
              } else {
                throw new TimeoutException("forced timeout")
              }
            }
          }
        }
      }

      override lazy val authenticator: Challenge = {
        new MockedShiroAuthenticator(testConfig.getConfig("authentication"))(ec, system).challenge()
      }
    }
    api.myRoutes
  }

  private val routesWithProxyUser = routesWithTimeout(true, 1000)
  private val routesWithoutProxyUser = routesWithTimeout(false, 1000)

  private val USER_NAME = "presidentskroob"
  private val USER_NAME_2 = USER_NAME + SparkJobUtils.NameContextDelimiter + "2"

  // set to some valid user
  private val authorization = new Authorization(new BasicHttpCredentials(USER_NAME, "12345"))
  private val authorizationInvalidPassword = new Authorization(new BasicHttpCredentials(USER_NAME, "xxx"))
  private val authorizationUnknownUser = new Authorization(new BasicHttpCredentials("whoami", "xxx"))
  private val dt = DateTime.parse("2013-05-29T00Z")
  private val jobInfo = JobInfo("foo-1", "cid", "context", "com.abc.meme",
      JobStatus.Running, dt, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt)))
  private val ResultKey = "result"

  private val addedContexts = new scala.collection.mutable.HashSet[String] with SynchronizedSet[String]

  class DummyActor extends Actor {
    import CommonMessages._

    def receive: PartialFunction[Any, Unit] = {
      case ListBinaries(_) => sender ! Map()
      case GetJobInfo(id) => sender ! Some(jobInfo)
      case GetJobResult(id) => sender ! JobResult(id, id + "!!!")
      case ContextSupervisor.ListContexts => sender ! addedContexts.toSeq
      case ContextSupervisor.AddContext(name, _) =>
        if (addedContexts.contains(name)) {
          sender ! ContextSupervisor.ContextAlreadyExists
        } else {
          addedContexts.add(name)
          sender ! ContextSupervisor.ContextInitialized
        }
      case ContextSupervisor.StopContext(name, force) =>
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
      Post("/jobs?appName=foo&classPath=com.abc.meme&context=one&sync=true", config2)
        .withHeaders(authorizationInvalidPassword) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(Unauthorized)
        }
    }

    it("should not allow unknown user") {
      Delete("/jobs/job_to_kill").withHeaders(authorizationUnknownUser) ~>
        sealRoute(routesWithProxyUser) ~> check {
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
      Post("/contexts/one?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .withHeaders(authorization) ~>
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
      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "one") &&
        !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "two") &&
        !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "three")) {
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
      Post("/contexts/" + USER_NAME + "_c2?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .withHeaders(authorization) ~>
        sealRoute(testRoutes) ~> check {
          status should be(OK)
        }

      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "c") &&
        !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + USER_NAME + "_c") &&
        !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + USER_NAME + "_c2")) {
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

      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "c1") &&
        !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter
          + SparkJobUtils.NameContextDelimiter + "2"
          + SparkJobUtils.NameContextDelimiter + "c2")) {
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
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }
      Thread.sleep(5)
      addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + cName) should equal(true)
      Post("/contexts/" + cName).withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(BadRequest)
        }
      //another proxy user should also not work
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XYZ")
        .withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(BadRequest)
        }
      addedContexts.clear
    }

    it("should allow contexts of the same name by different users") {
      val cName = "X"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .withHeaders(authorization) ~>
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
      Post("/contexts/validImpersonation?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .withHeaders(authorization) ~>
        sealRoute(routesWithoutProxyUser) ~> check {
          status should be(OK)
        }
      Delete("/contexts/validImpersonation").withHeaders(authorization) ~>
        sealRoute(routesWithoutProxyUser) ~> check {
          status should be(OK)
      }
    }

    it("should allow user with valid authorization to impersonate another user when flag is off") {
      val cName = "who"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XYZ")
        .withHeaders(authorization) ~>
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
      Delete("/contexts/xxx").withHeaders(authorizationUnknownUser) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(Unauthorized)
      }
    }

    it("should allow valid user to delete context with proper impersonation") {
      addedContexts.clear
      Post("/contexts/xxx?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .withHeaders(authorization) ~>
        sealRoute(routesWithProxyUser) ~> check {
          status should be(OK)
        }

      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "xxx")) {
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

    it("contexts should not allow user with valid authorization when timeout") {
      Get("/contexts").withHeaders(authorization) ~>
        sealRoute(routesWithTimeout(true, 0)) ~> check {
          status should be(InternalServerError)
        }
    }
  }
}

