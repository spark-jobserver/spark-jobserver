package spark.jobserver

import akka.actor.{Actor, ActorSystem, Props, PoisonPill}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import spark.jobserver.io.JobDAOActor
import spray.http.StatusCodes._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest


class HealthzApiSpec extends FunSpec with Matchers with BeforeAndAfterAll
with ScalatestRouteTest with HttpService {
  import spray.httpx.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._
  
  def actorRefFactory: ActorSystem = system
  val bindConfKey = "spark.jobserver.bind-address"
  val bindConfVal = "127.0.0.1"
  val masterConfKey = "spark.master"
  val masterConfVal = "spark://localhost:7077"
  val config = ConfigFactory.parseString(s"""
    spark {
      master = "$masterConfVal"
      jobserver.bind-address = "$bindConfVal"
      jobserver.short-timeout = 3 s
    }
    shiro {
      authentication = off
    }
                                 """)
  val StatusKey = "status"
  val ResultKey = "result"
  
  val dummyPort = 9999
  val aliveActor = system.actorOf(Props(classOf[AliveActor], this))
  val deadActor = system.actorOf(Props(classOf[DeadActor], this))
  val jobDaoActor = system.actorOf(JobDAOActor.props(new InMemoryDAO))
  val statusActor = system.actorOf(JobStatusActor.props(jobDaoActor))

  class AliveActor extends Actor {
    import CommonMessages._
    import ContextSupervisor._
    import JobInfoActor._
    import JobDAOActor.JobInfos
    import JobDAOActor.GetJobInfos

    def receive: PartialFunction[Any, Unit] = {
      case GetJobStatus("dummyjobid") =>
        sender ! NoSuchJobId
      case GetContext("dummycontext") =>
        sender ! NoSuchContext
      case GetJobInfos(1) =>
        sender ! JobInfos(null)
      case GetJobResult("dummyjobid") =>
        sender ! NoSuchJobId
    }
  }
  
  class DeadActor extends Actor {
    import CommonMessages._
    import ContextSupervisor._
    import JobInfoActor._
    import JobDAOActor.GetJobInfos

    def receive: PartialFunction[Any, Unit] = {
      case GetJobStatus("dummyjobid") =>
        self ! PoisonPill
      case GetContext("dummycontext") =>
        self ! PoisonPill
      case GetJobInfos(1) =>
        self ! PoisonPill
      case GetJobResult("dummyjobid") =>
        self ! PoisonPill
    }
  }
  
  describe("healthz - all actors alive") {
    val api = new WebApi(system, config, dummyPort, aliveActor, aliveActor, aliveActor, aliveActor, aliveActor)
    val routes = api.myRoutes
    it("should return OK") {
      Get("/healthz") ~> sealRoute(routes) ~> check {
        status should be (OK)
      }
    }
  }
  
  describe("healthz - all actors dead") {
    val api = new WebApi(system, config, dummyPort, deadActor, deadActor, deadActor, deadActor, deadActor)
    val routes = api.myRoutes
    it("should return 500") {
      Get("/healthz") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val resultMap = responseAs[Map[String, String]]
        resultMap(StatusKey) should be ("ERROR")
        resultMap(ResultKey) should be ("Required actors not alive")
      }
    }
  }
  
  describe("healthz - supervisor dead") {
    val api = new WebApi(system, config, dummyPort, aliveActor, aliveActor, deadActor, aliveActor, aliveActor)
    val routes = api.myRoutes
    it("should return 500") {
      Get("/healthz") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val resultMap = responseAs[Map[String, String]]
        resultMap(StatusKey) should be ("ERROR")
        resultMap(ResultKey) should be ("Required actors not alive")
      }
    }
  }
  
  describe("healthz - DAOactor dead") {
    val api = new WebApi(system, config, dummyPort, aliveActor, aliveActor, aliveActor, aliveActor, deadActor)
    val routes = api.myRoutes
    it("should return 500") {
      Get("/healthz") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val resultMap = responseAs[Map[String, String]]
        resultMap(StatusKey) should be ("ERROR")
        resultMap(ResultKey) should be ("Required actors not alive")
      }
    }
  }
  
  describe("healthz - DAO and JobInfo dead") {
    val api = new WebApi(system, config, dummyPort, aliveActor, aliveActor, aliveActor, deadActor, deadActor)
    val routes = api.myRoutes
    it("should return 500") {
      Get("/healthz") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val resultMap = responseAs[Map[String, String]]
        resultMap(StatusKey) should be ("ERROR")
        resultMap(ResultKey) should be ("Required actors not alive")
      }
    }
  }
  
}
