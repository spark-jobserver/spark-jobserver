package spark.jobserver

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import spark.jobserver.io.JobDAOActor.GetJobInfo
import spark.jobserver.io.{InMemoryBinaryObjectsDAO, InMemoryMetaDAO, JobDAOActor}
import spark.jobserver.util.ActorsHealthCheck
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class HealthzApiSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll
with ScalatestRouteTest with SprayJsonSupport {

  import spray.json._
  import DefaultJsonProtocol._
  
  def actorRefFactory: ActorSystem = system
  val bindConfKey = "spark.jobserver.bind-address"
  val bindConfVal = "127.0.0.1"
  val masterConfKey = "spark.master"
  val masterConfVal = "spark://localhost:7077"
  val healthConfVal = "spark.jobserver.util.ActorsHealthCheck"
  val config = ConfigFactory.parseString(s"""
    spark {
      master = "$masterConfVal"
      jobserver.bind-address = "$bindConfVal"
      jobserver.short-timeout = 3 s
      jobserver.healthcheck = "$healthConfVal"
    }
                                 """)
  val StatusKey = "status"
  val ResultKey = "result"
  
  val dummyPort = 9999
  val aliveActor = system.actorOf(Props(classOf[AliveActor], this))
  val deadActor = system.actorOf(Props(classOf[DeadActor], this))
  val inMemoryMetaDAO = new InMemoryMetaDAO
  val inMemoryBinDAO = new InMemoryBinaryObjectsDAO
  val daoConfig: Config = ConfigFactory.load("local.test.combineddao.conf")
  val jobDaoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
  val statusActor = system.actorOf(JobStatusActor.props(jobDaoActor))

  class AliveActor extends Actor {
    import CommonMessages._
    import ContextSupervisor._
    import JobDAOActor.{GetJobInfos, JobInfos}

    def receive: PartialFunction[Any, Unit] = {
      case GetJobInfo("dummyjobid") =>
        sender ! None
      case GetContext("dummycontext") =>
        sender ! NoSuchContext
      case GetJobInfos(1, None) =>
        sender ! JobInfos(null)
    }
  }
  
  class DeadActor extends Actor {
    import CommonMessages._
    import ContextSupervisor._
    import JobDAOActor.GetJobInfos

    def receive: PartialFunction[Any, Unit] = {
      case GetJobInfo("dummyjobid") =>
        self ! PoisonPill
      case GetContext("dummycontext") =>
        self ! PoisonPill
      case GetJobInfos(1, None) =>
        self ! PoisonPill
    }
  }
  
  describe("healthz - all actors alive") {
    val healthCheckInst = new ActorsHealthCheck(aliveActor, aliveActor)
    val api = new WebApi(system, config, dummyPort, aliveActor, aliveActor, aliveActor, aliveActor, healthCheckInst)
    val routes = api.myRoutes
    it("should return OK") {
      Get("/healthz") ~> sealRoute(routes) ~> check {
        status should be (OK)
      }
    }
  }
  
  describe("healthz - all actors dead") {
    val healthCheckInst = new ActorsHealthCheck(deadActor, deadActor)
    val api = new WebApi(system, config, dummyPort, deadActor, deadActor, deadActor, deadActor, healthCheckInst)
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
    val healthCheckInst = new ActorsHealthCheck(deadActor, aliveActor)
    val api = new WebApi(system, config, dummyPort, aliveActor, aliveActor, deadActor, aliveActor, healthCheckInst)
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
  
  describe("healthz - JobDaoActor dead") {
    val healthCheckInst = new ActorsHealthCheck(aliveActor, deadActor)
    val api = new WebApi(system, config, dummyPort, aliveActor, aliveActor, aliveActor, deadActor, healthCheckInst)
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
