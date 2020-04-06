package spark.jobserver.util

import akka.actor.{ActorIdentity, ActorRef, ActorSystem, Identify}
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import spark.jobserver.ContextSupervisor

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._
import spray.http.StatusCodes
import spray.routing.{HttpService, RequestContext}
import spark.jobserver.io.JobInfo
import spark.jobserver.io.JobDAOActor

trait HealthCheck {

  def isHealthy() : Boolean

}

class ActorsHealthCheck(supervisor: ActorRef, daoActor: ActorRef)
    extends HealthCheck {

    val logger = LoggerFactory.getLogger(getClass)

    def isHealthy() : Boolean = {
      logger.info("Receiving healthz check request")
      import spark.jobserver.ContextSupervisor._
      import JobDAOActor._
      import spark.jobserver.CommonMessages._
      var actorsAlive: Int = 0
      implicit val duration: Timeout = 30 seconds
      val supervisorFuture = (supervisor ? GetContext("dummycontext")).mapTo[NoSuchContext.type]
      val jobDaoFuture = (daoActor ? GetJobInfos(1, None)).mapTo[JobInfos]
      val jobResultFuture = (for {
        resultActor <- (supervisor ? GetResultActor("getDefaultGlobalActor")).mapTo[ActorRef]
        result <- resultActor ? GetJobResult("dummyjobid")
      } yield result).mapTo[NoSuchJobId.type]


      val listOfFutures = Seq(supervisorFuture, jobDaoFuture, jobResultFuture)
      val futureOfList = Future.sequence(listOfFutures)
      try {
        val results = Await.result(futureOfList, 60 seconds)
        for (result <- results) {
          result match {
            case NoSuchContext => actorsAlive += 1
            case NoSuchJobId => actorsAlive += 1
            case JobInfos(jobInfos) => actorsAlive += 1
            case _ => logger.error("Unexpected response!")
         }
        }
      } catch {
        case ex: Exception => logger.error(ex.getMessage())
      }

      if (actorsAlive == listOfFutures.length) {
        logger.info("Required actors alive")
        true
      }
      else {
        logger.error("Required actors not alive")
        false
      }
  }
}

class APIHealthCheck extends HealthCheck {

    def isHealthy() : Boolean = {
        true
    }

}

