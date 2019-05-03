package spark.jobserver

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._
import spray.http.StatusCodes
import spray.routing.{HttpService, RequestContext}
import spark.jobserver.io.JobInfo
import spark.jobserver.io.JobDAOActor

trait HealthCheckService {

  def healthCheckImpl(ctx: RequestContext, supervisor: ActorRef, jobInfo: ActorRef,
                      daoActor: ActorRef)

}

class ActorsHealthCheck extends HealthCheckService {

    val logger = LoggerFactory.getLogger(getClass)

    def errMap(errMsg: String) : Map[String, String] = Map("status" -> "ERROR", "result" -> errMsg)

    def healthCheckImpl(ctx: RequestContext, supervisor: ActorRef, jobInfo: ActorRef,
                        daoActor: ActorRef) {
      logger.info("Receiving healthz check request")
      import ContextSupervisor._
      import JobManagerActor._
      import JobInfoActor._
      import JobDAOActor._
      import CommonMessages._
      var actorsAlive: Int = 0
      implicit val duration: Timeout = 3 seconds
      val supervisorFuture = (supervisor ? GetContext("dummycontext")).mapTo[NoSuchContext.type]
      val jobInfofuture = (jobInfo ? GetJobStatus("dummyjobid")).mapTo[NoSuchJobId.type]
      val jobDaofuture = (daoActor ? GetJobInfos(1)).mapTo[JobInfos]
      val jobResultfuture = (jobInfo ? GetJobResult("dummyjobid")).mapTo[NoSuchJobId.type]

      val listOfFutures = Seq(supervisorFuture, jobInfofuture, jobDaofuture, jobResultfuture)
      val futureOfList = Future.sequence(listOfFutures)

      try {
        val results = Await.result(futureOfList, 30 seconds)
        for (result <- results) {
          result match {
            case NoSuchContext => actorsAlive += 1
            case NoSuchJobId => actorsAlive += 1
            case JobInfos(jobInfos) => actorsAlive += 1
            case unexpected => logger.error("Received: " + unexpected)
         }
        }
      } catch {
        case NonFatal(ex) => logger.error(ex.getMessage())
      }

      if (actorsAlive == 4) {
        logger.info("Required actors alive")
        ctx.complete(StatusCodes.OK)
      }
      else {
        logger.error("Required actors not alive")
        ctx.complete(500, errMap("Required actors not alive"))
      }
  }
}

class APIHealthCheck extends HealthCheckService {

    def healthCheckImpl(ctx: RequestContext, supervisor: ActorRef, jobInfo: ActorRef,
                        daoActor: ActorRef) {
        ctx.complete(StatusCodes.OK)
    }

}
