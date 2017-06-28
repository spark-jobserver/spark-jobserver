package spark.jobserver.services

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigRenderOptions}
import spray.json._

import spark.jobserver.CommonMessages.NoSuchJobId
import spark.jobserver.JobInfoActor.{GetJobConfig, GetJobStatuses}
import spark.jobserver.io.JobInfo
import spark.jobserver.metrics.ServiceMetrics

trait JobServiceJson extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val printer     = PrettyPrinter
  implicit val respFormat  = jsonFormat7(JobInfoResponse)
  implicit val noJobFormat = jsonFormat1(JobNotFound)
}

case class JobInfoResponse(jobId: String,
                           startTime: String,
                           endTime: Option[String],
                           classPath: String,
                           context: String,
                           duration: Option[String],
                           status: String)

case class JobNotFound(message: String)

trait JobService extends BaseService with JobServiceJson with ServiceMetrics {

  val jobActor: ActorRef

  private val rendering = ConfigRenderOptions
    .defaults()
    .setComments(false)
    .setOriginComments(false)

  private def buildResponse(j: JobInfo): JobInfoResponse = {
    val length = j.jobLengthMillis.map(Duration(_, MILLISECONDS).toSeconds).map(d => s"${d}s")
    val status = if (length.nonEmpty) "DONE" else "RUNNING"
    val end    = j.endTime.map(_.toString)
    JobInfoResponse(j.jobId, j.startTime.toString, end, j.classPath, j.contextName, length, status)
  }

  private def getAllJobInfo(limit: Int, status: Option[String]): Future[Seq[JobInfoResponse]] = {
    (jobActor ? GetJobStatuses(Some(limit), status))
      .mapTo[Seq[JobInfo]]
      .map(_.map(buildResponse))
  }

  private def getJobConfig(id: Int) = {
    (jobActor ? GetJobConfig(id.toString)).map {
      case NoSuchJobId => JobNotFound(s"Job Id: $id Not Found").toJson.toString
      case cfg: Config => cfg.root.render(rendering)
    }
  }

  def jobRoutes: Route = {
    pathPrefix("jobs") {
      pathEndOrSingleSlash {
        traceName("All-Jobs-Get") {
          (get & parameters('limit.as[Int] ? 10, 'status.as[String] ?)) { (limit, status) =>
            complete(getAllJobInfo(limit, status))
          }
        } ~
        traceName("Jobs-Post") {
          post {
            complete("")
          }
        }
      } ~
      pathPrefix(IntNumber) { id =>
        pathEndOrSingleSlash {
          traceName("Specific-Job") {
            get {
              complete(s"$id")
            } ~
            delete {
              complete(s"$id")
            }
          }
        } ~
        pathPrefix("config") {
          pathEndOrSingleSlash {
            complete(getJobConfig(id))
          }
        }
      }
    }
  }
}
