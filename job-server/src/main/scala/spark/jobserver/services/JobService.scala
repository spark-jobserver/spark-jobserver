package spark.jobserver.services

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import spray.json._

import spark.jobserver.JobInfoActor.GetJobStatuses
import spark.jobserver.io.JobInfo

trait JobServiceJson extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val printer    = PrettyPrinter
  implicit val respFormat = jsonFormat7(JobInfoResponse)
}

case class JobInfoResponse(jobId: String,
                           startTime: String,
                           endTime: Option[String],
                           classPath: String,
                           context: String,
                           duration: Option[String],
                           status: String)

trait JobService extends BaseService with JobServiceJson {

  val jobActor: ActorRef

  private def buildResponse(j: JobInfo): JobInfoResponse = {
    val length = j.jobLengthMillis.map(Duration(_, MILLISECONDS).toSeconds).map(d => s"${d}s")
    val status = if (length.nonEmpty) "DONE" else "RUNNING"
    val end    = j.endTime.map(_.toString)
    JobInfoResponse(j.jobId, j.startTime.toString, end, j.classPath, j.contextName, length, status)
  }

  private def getAllJobInfo(limit: Int, status: Option[String]): Future[Seq[JobInfoResponse]] = {
    jobActor
      .?(GetJobStatuses(Some(limit), status))
      .mapTo[Seq[JobInfo]]
      .map(_.map(buildResponse))
  }

  def jobRoutes: Route = {
    pathPrefix("jobs") {
      pathEndOrSingleSlash {
        (get & parameters('limit.as[Int] ? 10, 'status.as[String] ?)) { (limit, status) =>
          complete(getAllJobInfo(limit, status))
        } ~
          post {
            complete("")
          }
      } ~
        pathPrefix(IntNumber) { id =>
          pathEndOrSingleSlash {
            get {
              complete(s"$id")
            } ~
              delete {
                complete(s"$id")
              }
          } ~
            pathPrefix("config") {
              pathEndOrSingleSlash {
                complete(s"$id/config")
              }
            }
        }
    }
  }
}
