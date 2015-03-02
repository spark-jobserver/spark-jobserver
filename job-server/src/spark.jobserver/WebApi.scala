package spark.jobserver

import akka.actor.{ActorRefFactory, ActorSystem, ActorRef}

import com.gettyimages.spray.swagger.SwaggerHttpService

import com.typesafe.config.Config

import com.wordnik.swagger.model.ApiInfo

import ooyala.common.akka.web.{ WebService, CommonRoutes }

import org.slf4j.LoggerFactory

import spark.jobserver.util.SparkJobUtils

import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._
import spray.routing.{ HttpService, Route }

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import scala.util.Try

class WebApi(system: ActorSystem,
             override val config: Config,
             port: Int,
             override val jarManager: ActorRef,
             override val supervisor: ActorRef,
             override val jobInfo: ActorRef)
  extends HttpService with CommonRoutes with JarRoutes with CommonRouteBehaviour with ContextRoutes
    with JobRoutes {
  import CommonMessages._
  import ContextSupervisor._
  import scala.concurrent.duration._

  // Get spray-json type classes for serializing Map[String, Any]
  import ooyala.common.akka.web.JsonUtils._

  override def actorRefFactory: ActorSystem = system
  override implicit val ec: ExecutionContext = system.dispatcher

  override val contextTimeout = SparkJobUtils.getContextTimeout(config)
  val sparkAliveWorkerThreshold = Try(config.getInt("spark.jobserver.sparkAliveWorkerThreshold")).getOrElse(1)
  val bindAddress = config.getString("spark.jobserver.bind-address")

  val logger = LoggerFactory.getLogger(getClass)

  val swaggerService = new SwaggerHttpService {
    override def apiTypes : Seq[Type] = Seq(typeOf[JarRoutes], typeOf[ContextRoutes], typeOf[JobRoutes])
    override def apiVersion : String = "2.0"
    override def baseUrl : String = "/" // let swagger-ui determine the host and port
    override def docsPath : String = "api-docs"
    override def actorRefFactory : ActorRefFactory = WebApi.this.actorRefFactory
    override def apiInfo : Option[ApiInfo] = Some(new ApiInfo("Spark Job-Server",
      "Provides a RESTful interface for submitting and managing Apache Spark jobs, jars, and job contexts",
      "https://github.com/spark-jobserver/spark-jobserver",
      "",
      "Apache V2",
      "http://www.apache.org/licenses/LICENSE-2.0"))

    //authorizations, not used
  }

  val myRoutes = jarRoutes ~ contextRoutes ~ jobRoutes ~ healthzRoutes ~ otherRoutes ~ swaggerService.routes ~
    get {
      implicit val ar: ActorSystem = actorRefFactory
      pathPrefix("docs") { pathEndOrSingleSlash {
        getFromResource("swagger-ui/index.html")
      }
      } ~ {
        getFromResourceDirectory("swagger-ui")
      }
    }


  def start() {
    logger.info("Starting browser web service...")
    WebService.start(myRoutes ~ commonRoutes, system, bindAddress, port)
  }

  /**
   * Routes for getting health status of job server
   *    GET /healthz              - return OK or error message
   */
  def healthzRoutes: Route = pathPrefix("healthz") {
    get { ctx =>
      logger.info("Receiving healthz check request")
      ctx.complete("OK")
    }
  }

  def otherRoutes: Route = get {
    implicit val ar = actorRefFactory

    path("") {
      // Main index.html page
      getFromResource("html/index.html")
    } ~ pathPrefix("html") {
      // Static files needed by index.html
      getFromResourceDirectory("html")
    }
  }

}
