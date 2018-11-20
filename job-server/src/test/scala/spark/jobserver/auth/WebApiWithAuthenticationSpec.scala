package spark.jobserver.auth

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestKit
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import spark.jobserver._
import spark.jobserver.util.SparkJobUtils
import spark.jobserver.io.{BinaryInfo, BinaryType, JobInfo, JobStatus}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, HttpCredentials}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.shiro.config.IniSecurityManagerFactory
import org.apache.shiro.SecurityUtils
import org.apache.shiro.config.Ini

import scala.collection.mutable.SynchronizedSet
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import org.slf4j.LoggerFactory
import akka.http.scaladsl.model.{HttpEntity, Uri}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.directives.SecurityDirectives.AuthenticationResult

// Tests authorization only, actual responses are tested elsewhere
// Does NOT test underlying Supervisor / JarManager functionality
class WebApiWithAuthenticationSpec extends FunSpec
  with Matchers with BeforeAndAfterAll with ScalatestRouteTest {
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  def actorRefFactory: ActorSystem = system

  val bindConfKey = "spark.jobserver.bind-address"
  val bindConfVal = "127.0.0.1"
  val masterConfKey = "spark.master"
  val masterConfVal = "spark://localhost:7079"
  val config = ConfigFactory.parseString(s"""
    spark {
      master = "$masterConfVal"
      jobserver.bind-address = "$bindConfVal"
      jobserver.short-timeout = 3 s
    }
    shiro {
      authentication = on
    }
                                 """)

  val dummyPort = 9997

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  // See http://doc.akka.io/docs/akka/2.2.4/scala/actors.html#Deprecated_Variants;
  // for actors declared as inner classes we need to pass this as first arg
  private val dummyActor = system.actorOf(Props(classOf[DummyActor], this))

  private def routesWithTimeout(useAsProxyUser: Boolean, authTimeout: Int): server.Route = {
    val testConfig = config
      .withValue("shiro.authentication-timeout", ConfigValueFactory.fromAnyRef(authTimeout))
      .withValue("shiro.use-as-proxy-user", ConfigValueFactory.fromAnyRef(useAsProxyUser))
    val api = new WebApi(system, testConfig, dummyPort, dummyActor, dummyActor, dummyActor, dummyActor) {
      private def asShiroAuthenticatorWithWait(authTimeout: Int)
                                              (implicit ec: ExecutionContext,
                                               s: ActorSystem):
      Option[HttpCredentials] => Future[AuthenticationResult[User]] = {
        val logger = LoggerFactory.getLogger(getClass)

        credentials: Option[HttpCredentials] => {
          lazy val f = Future {
            credentials match {
              case Some(creds) if explicitValidation(creds, logger) =>
                Right(User(creds.asInstanceOf[BasicHttpCredentials].username))
              case Some(_) =>
                Left(challenge)
              case None =>
                Left(challenge)
            }
          }
          import scala.concurrent.duration._
          lazy val t = akka.pattern.after(duration = authTimeout second,
            using = s.scheduler)(Future.failed(new TimeoutException("Authentication timed out!")))

          Future firstCompletedOf Seq(f, t)
        }
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

      override lazy val authenticator: Option[HttpCredentials] => Future[AuthenticationResult[User]] = {
        logger.info("Using authentication.")
        initSecurityManager()
        asShiroAuthenticatorWithWait(authTimeout)(ec = system.dispatcher, s = system)
      }
    }
    api.myRoutes
  }

  private val routesWithProxyUser = routesWithTimeout(useAsProxyUser = true, 1000)
  private val routesWithoutProxyUser = routesWithTimeout(useAsProxyUser = false, 1000)

  private val USER_NAME = "presidentskroob"
  private val USER_NAME_2 = USER_NAME + SparkJobUtils.NameContextDelimiter + "2"

  // set to some valid user
  private val authorization = new Authorization(new BasicHttpCredentials(USER_NAME, "12345"))
  private val authorizationInvalidPassword = new Authorization(new BasicHttpCredentials(USER_NAME, "xxx"))
  private val authorizationUnknownUser =
    new Authorization(new BasicHttpCredentials("whoami", "xxx"))
  private val dt = DateTime.parse("2013-05-29T00Z")
  private val jobInfo =
    JobInfo("foo-1", "cid", "context",
      BinaryInfo("demo", BinaryType.Jar, dt), "com.abc.meme", JobStatus.Running, dt, None, None)
  private val ResultKey = "result"

  private val addedContexts = new scala.collection.mutable.HashSet[String] with SynchronizedSet[String]

  class DummyActor extends Actor {
    import CommonMessages._
    import JobInfoActor._

    def receive: PartialFunction[Any, Unit] = {
      case ListBinaries(_) => sender ! Map()
      case GetJobStatus(id) => sender ! jobInfo
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

  describe("jars routes") {
    it("should allow user with valid authorization") {
      Get("/jars").addHeader(authorization) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }
    }

    it("should not allow user with invalid password") {
      Post("/jars/foobar", Array[Byte](0, 1, 2))
        .addHeader(authorizationInvalidPassword) ~>
        Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }

    it("should not allow unknown user") {
      Get("/jars").addHeader(authorizationUnknownUser) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }
  }

  describe("binaries routes") {

    it("should allow user with valid authorization") {
      Get("/binaries").addHeader(authorization) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }
    }

    it("should not allow user with invalid password") {
      Post("/binaries/pyfoo", HttpEntity(BinaryType.Egg.contentType, Array[Byte](0, 1, 2)))
        .addHeader(authorizationInvalidPassword) ~>
        Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }

    it("should not allow unknown user") {
      Post("/binaries/pyfoo", HttpEntity(BinaryType.Egg.contentType, Array[Byte](0, 1, 2)))
        .addHeader(authorizationUnknownUser) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }
  }

  describe("/jobs routes") {
    it("should allow user with valid authorization") {
      Get("/jobs/foobar").addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }
    }

    it("should not allow user with invalid password") {
      val config2 = "foo.baz = booboo"
      Post("/jobs?appName=foo&classPath=com.abc.meme&context=one&sync=true", config2)
        .addHeader(authorizationInvalidPassword) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }

    it("should not allow unknown user") {
      Delete("/jobs/job_to_kill").addHeader(authorizationUnknownUser) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }
  }

  describe("context routes") {
    it("should allow user with valid authorization") {
      Get("/contexts").addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }
    }

    it("should only show visible contexts to user") {
      val testRoutes = routesWithProxyUser
      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      addedContexts.clear
      //the parameter SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME should have no effect
      Post("/contexts/one?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .addHeader(authorization) ~>
      Route.seal(testRoutes) ~> check {
        println(responseAs[String])
        status should be(OK)
      }
      Post("/contexts/two").addHeader(authorization) ~>
      Route.seal(testRoutes) ~> check {
        status should be(OK)
      }
      //again, proxy user param should have no effect
      Post("/contexts/three?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XXX").addHeader(authorization) ~>
      Route.seal(testRoutes) ~> check {
        status should be(OK)
      }
      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "one") &&
             !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "two") &&
             !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "three")) {
        Thread.sleep(3)
      }

      //this user should see both contexts
      Get("/contexts/").addHeader(authorization) ~> Route.seal(testRoutes) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("one", "two", "three"))
      }

      //another user should not see any contexts
      Get("/contexts/").addHeader(authorization2) ~> Route.seal(testRoutes) ~> check {
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
      Post("/contexts/" + USER_NAME + "_c").addHeader(authorization) ~>
      Route.seal(testRoutes) ~> check {
        status should be(OK)
      }
      //add same context without the user name prefix
      Post("/contexts/" + "c").addHeader(authorization) ~>
      Route.seal(testRoutes) ~> check {
        status should be(OK)
      }
      //add an impersonating contexts that already has the user name as a prefix
      Post("/contexts/" + USER_NAME + "_c2?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .addHeader(authorization) ~>
      Route.seal(testRoutes) ~> check {
        status should be(OK)
      }

      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "c") &&
             !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + USER_NAME + "_c") &&
             !addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + USER_NAME + "_c2")) {
        Thread.sleep(3)
      }

      Get("/contexts/").addHeader(authorization) ~> Route.seal(testRoutes) ~> check {
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
      Post("/contexts/c1").addHeader(authorization) ~>
      Route.seal(testRoutes) ~> check {
        status should be(OK)
      }
      //add another context as USER_NAME_2
      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      Post("/contexts/c2").addHeader(authorization2) ~>
      Route.seal(testRoutes) ~> check {
        status should be(OK)
      }

      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "c1") &&
             !addedContexts.contains(
               USER_NAME + SparkJobUtils.NameContextDelimiter
               + SparkJobUtils.NameContextDelimiter + "2"
               + SparkJobUtils.NameContextDelimiter + "c2"
             )) {
        Thread.sleep(3)
      }

      Get("/contexts/").addHeader(authorization) ~> Route.seal(testRoutes) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("c1"))
      }
      Get("/contexts/").addHeader(authorization2) ~> Route.seal(testRoutes) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("c2"))
      }
      addedContexts.clear
    }

    //test what happens when user uses the proxy user param context with the same name?
    it("should ignore proxy user param") {
      val cName = "Z"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }
      Thread.sleep(5)
      addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + cName) should equal(true)
      Post("/contexts/" + cName).addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(BadRequest)
      }
      //another proxy user should also not work
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XYZ")
        .addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(BadRequest)
      }
      addedContexts.clear
    }

    it("should allow contexts of the same name by different users") {
      val cName = "X"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }
      Thread.sleep(5)
      Get("/contexts/").addHeader(authorization) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set(cName))
      }

      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      Post("/contexts/" + cName).addHeader(authorization2) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }

      Thread.sleep(1)
      Get("/contexts/").addHeader(authorization) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set(cName))
      }
      Get("/contexts/").addHeader(authorization2) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set(cName))
      }
      addedContexts.clear
    }

    it("should not allow user with invalid password") {
      Post("/contexts/one").addHeader(authorizationInvalidPassword) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }

    it("should allow user with valid authorization to impersonate herself") {
      Post(
        Uri("/contexts/validImpersonation")
          .withQuery(Query(s"${SparkJobUtils.SPARK_PROXY_USER_PARAM}=$USER_NAME")))
        .addHeader(authorization) ~>
      Route.seal(routesWithoutProxyUser) ~> check {
        status should be(OK)
      }
      Delete("/contexts/validImpersonation").addHeader(authorization) ~>
      Route.seal(routesWithoutProxyUser) ~> check {
        status should be(OK)
      }
    }

    it("should allow user with valid authorization to impersonate another user when flag is off") {
      val cName = "who"
      Post("/contexts/" + cName + "?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=XYZ")
        .addHeader(authorization) ~>
      Route.seal(routesWithoutProxyUser) ~> check {
        status should be(OK)
      }
      //any user should be able to see this context
      val authorization2 = new Authorization(new BasicHttpCredentials(USER_NAME_2, "ludicrousspeed"))
      Get("/contexts/").addHeader(authorization2) ~> Route.seal(routesWithoutProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]].contains(cName) should be(true)
      }
      Get("/contexts/").addHeader(authorization) ~> Route.seal(routesWithoutProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]].contains(cName) should be(true)
      }
    }

    it("should not allow unknown user") {
      Delete("/contexts/xxx").addHeader(authorizationUnknownUser) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(Unauthorized)
      }
    }

    it("should allow valid user to delete context with proper impersonation") {
      addedContexts.clear
      Post("/contexts/xxx?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=" + USER_NAME)
        .addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }

      while (!addedContexts.contains(USER_NAME + SparkJobUtils.NameContextDelimiter + "xxx")) {
        Thread.sleep(3)
      }
      Get("/contexts/").addHeader(authorization) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set("xxx"))
      }
      Delete("/contexts/xxx").addHeader(authorization) ~>
      Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
      }
      Get("/contexts/").addHeader(authorization) ~> Route.seal(routesWithProxyUser) ~> check {
        status should be(OK)
        responseAs[Set[String]] should be(Set())
      }
    }
  }

  describe("routes with timeout") {
    it("jobs should not allow user with valid authorization when timeout") {
      Get("/jobs/foobar").addHeader(authorization) ~>
      Route.seal(routesWithTimeout(useAsProxyUser = true, 0)) ~> check {
        status should be(InternalServerError)
      }
    }

    it("jars should not allow user with valid authorization when timeout") {
      Get("/jars").addHeader(authorization) ~>
      Route.seal(routesWithTimeout(useAsProxyUser = false, 0)) ~> check {
        status should be(InternalServerError)
      }
    }

    it("contexts should not allow user with valid authorization when timeout") {
      Get("/contexts").addHeader(authorization) ~>
      Route.seal(routesWithTimeout(useAsProxyUser = true, 0)) ~> check {
        status should be(InternalServerError)
      }
    }
  }
}
