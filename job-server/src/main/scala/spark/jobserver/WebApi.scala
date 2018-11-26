package spark.jobserver
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import spark.jobserver.common.akka.web.JsonUtils._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshalling.ToResponseMarshallable._

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpCredentials, RawHeader}
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.config._
import javax.net.ssl.SSLContext
import org.apache.shiro.SecurityUtils
import org.apache.shiro.config.IniSecurityManagerFactory
import org.slf4j.{Logger, LoggerFactory}
import spark.jobserver.WebApiUtils._
import spark.jobserver.auth._
import spark.jobserver.common.akka.web.{CommonRoutes, WebService}
import spark.jobserver.routes._
import spark.jobserver.util.{SSLContextFactory, SparkJobUtils}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object WebApi {
  val ResultKeyStartBytes: Array[Byte] = "{\n".getBytes
  val ResultKeyEndBytes: Array[Byte] = "}".getBytes
  val ResultKeyBytes: Array[Byte] = ("\"" + ResultKey + "\":").getBytes

  }

class WebApi(system: ActorSystem, config: Config, port: Int,
             binaryManagerActor: ActorRef, dataManager: ActorRef,
             supervisorActor: ActorRef, jobInfoActor: ActorRef)
  extends CommonRoutes
  with DataRoutes with BinaryRoutes with JarRoutes with ContextRoutes
  with JobRoutes
  with SJSAuthenticator {
  import CommonMessages._
  import ContextSupervisor._
  import WebApi._

  import scala.concurrent.duration._

  // Get spray-json type classes for serializing Map[String, Any]
  import spark.jobserver.common.akka.web.JsonUtils._


  val binaryManager: ActorRef = binaryManagerActor
  val supervisor: ActorRef = supervisorActor
  val jobInfo: ActorRef = supervisorActor

  def actorRefFactory: ActorSystem = system
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val actorSystem: ActorSystem = system
  implicit val ShortTimeout: Timeout =
    Timeout(
      config.getDuration("spark.jobserver.short-timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )

  val ResultChunkSize: Int = Option("spark.jobserver.result-chunk-size")
    .filter(config.hasPath)
    .fold(100 * 1024)(config.getBytes(_).toInt)

  val contextTimeout: Int = SparkJobUtils.getContextCreationTimeout(config)
  val contextDeletionTimeout: Int =
    SparkJobUtils.getContextDeletionTimeout(config)
  val bindAddress: String = config.getString("spark.jobserver.bind-address")

  override val logger: Logger = LoggerFactory.getLogger(getClass)

  val rejectionHandler: RejectionHandler = corsRejectionHandler withFallback RejectionHandler.default
  val exceptionHandler = ExceptionHandler {
    case e: Exception =>
      complete(
        StatusCodes.InternalServerError, errMap(e, "ERROR")
      )
  }

  val handleErrors: Directive0 = handleRejections(rejectionHandler) & handleExceptions(exceptionHandler)

  val myRoutes: Route = cors() {
    handleErrors {
      overrideMethodWithParameter("_method") {
          binaryRoutes(authenticator) ~
          jarRoutes(authenticator) ~
          contextRoutes(authenticator, config, contextTimeout, contextDeletionTimeout) ~
          jobRoutes(authenticator, config, contextTimeout) ~
          dataRoutes ~ healthzRoutes ~ otherRoutes
      }
    }
  }

  def authenticatorBuilder: Option[HttpCredentials] => Future[AuthenticationResult[User]] = {
    if (config.getBoolean("shiro.authentication")) {
      import java.util.concurrent.TimeUnit
      logger.info("Using authentication.")
      initSecurityManager()
      val authTimeout = Try(
        config
          .getDuration("shiro.authentication-timeout", TimeUnit.MILLISECONDS)
          .toInt / 1000
      ).getOrElse(10)
      asShiroAuthenticator(authTimeout)
    } else {
      logger.info("No authentication.")
      asAllUserAuthenticator
    }
  }

  lazy val authenticator
    : Option[HttpCredentials] => Future[AuthenticationResult[User]] = authenticatorBuilder

  /**
    * possibly overwritten by test
    */
  def initSecurityManager() {
    val sManager = new IniSecurityManagerFactory(
      config.getString("shiro.config.path")
    ).getInstance()
    SecurityUtils.setSecurityManager(sManager)
  }

  def start() {

    /**
      * activates ssl or tsl encryption between client and SJS if so requested
      * in config
      */
    implicit val sslContext: SSLContext = {
      SSLContextFactory.createContext(config.getConfig("akka.http.server"))
    }

    logger.info("Starting browser web service...")
    WebService.start(myRoutes ~ commonRoutes, system, bindAddress, port)
  }

  def extractContentTypeHeader: HttpHeader => Option[ContentType] = {
    case HttpHeader("Content-Type", value) => Some(ContentType.parse(value)) match {
      case Some(Right(v)) => Some(v)
      case Some(Left(v)) => throw new IllegalArgumentException(v.mkString)
    }
    case h: RawHeader if h.name == "Content-Type" => Some(ContentType.parse(h.value)) match {
      case Some(Right(v)) => Some(v)
      case Some(Left(v)) => throw new IllegalArgumentException(v.mkString)
    }
    case _ => None
  }

  val contentType: Directive1[Option[ContentType]] = optionalHeaderValue(extractContentTypeHeader)

  /**
    * Routes for listing, deletion of and storing data files
    *    GET /data                     - lists all currently stored files
    *    DELETE /data/<filename>       - deletes given file, no-op if file does not exist
    *    POST /data/<filename-prefix>  - upload a new data file, using the given prefix,
    *                                      a time stamp is appended to ensure uniqueness
    *
    * @author TimMaltGermany
    */
  def dataRoutes: Route = pathPrefix("data") {
    // user authentication
    authenticateOrRejectWithChallenge(authenticator) { _ =>
      dataRoutes(dataManager)
    }
  }


  /**
    * Routes for getting health status of job server
    *    GET /healthz              - return OK or error message
    */
  def healthzRoutes: Route = pathPrefix("healthz") {
    //no authentication required
    get {
      logger.info("Receiving healthz check request")
      complete("OK")
    }
  }

  def otherRoutes: Route = {
    import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
//    implicit val ar = actorRefFactory
    //no authentication required

    path("") {
      // Main index.html page
      getFromResource("html/index.html")
    } ~ pathPrefix("html") {
      // Static files needed by index.html
      getFromResourceDirectory("html")
    }
  }

  val errorEvents: Set[Class[_]] =
    Set(classOf[JobErroredOut], classOf[JobValidationFailed])
  val asyncEvents: Set[Class[_]] = Set(classOf[JobStarted]) ++ errorEvents
  val syncEvents: Set[Class[_]] = Set(classOf[JobResult]) ++ errorEvents

  /**
    * if the shiro user is to be used as the proxy user, then this
    * computes the context name from the user name (and a prefix) and appends the user name
    * as the spark proxy user parameter to the config
    */
  override protected def determineProxyUser(aConfig: Config,
                                  authInfo: AuthInfo, contextName: String): (String, Config) = {
    if (config.getBoolean("shiro.authentication") && config.getBoolean(
          "shiro.use-as-proxy-user")) {
      //proxy-user-param is ignored and the authenticated user name is used
      val config = aConfig.withValue(SparkJobUtils.SPARK_PROXY_USER_PARAM,
        ConfigValueFactory.fromAnyRef(authInfo.toString))
      (SparkJobUtils.userNamePrefix(authInfo.toString) + contextName, config)
    } else { (contextName, aConfig) }
  }



  override protected def getContextConfig(baseConfig: Config,
                                          fallbackParams: Map[String, String] = Map()): Config = {
    import collection.JavaConverters.mapAsJavaMapConverter
    Try(baseConfig.getConfig("spark.context-settings"))
      .getOrElse(ConfigFactory.empty)
      .withFallback(ConfigFactory.parseMap(fallbackParams.asJava))
      .resolve()
  }

}