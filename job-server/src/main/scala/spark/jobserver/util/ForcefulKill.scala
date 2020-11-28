package spark.jobserver.util

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.{Get, Post}
import akka.http.scaladsl.model.{FormData, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

trait ForcefulKill {
  def kill(): Unit
}

class StandaloneForcefulKill(config: Config, appId: String) extends ForcefulKill {
  private val logger = LoggerFactory.getLogger(getClass)

  override def kill(): Unit = {
    implicit val system = ActorSystem()
    implicit val ec = system.dispatcher
    implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))
    val sparkMasterString = config.getString("spark.master")
    val mode = sparkMasterString.split("//")
    if (!mode.head.containsSlice("spark")) throw new NotStandaloneModeException()

    val mastersIPsWithPort = mode.drop(1).head.split(",")
    val masterIPs = mastersIPsWithPort.map(_.split(":").filterNot(_.trim().equals("")).head)

    masterIPs.foreach {
      masterIP =>
        val baseSparkURL = s"http://$masterIP:8080"
        val getJsonRequest = Get(s"${baseSparkURL}/json/")

        getHTTPResponse(getJsonRequest).map {
          getJsonResponse =>
            Unmarshal(getJsonResponse.entity).to[String].map(ent => {
              val status = ent.parseJson.asJsObject.getFields("status").mkString
              status.contains("ALIVE") match {
                case true =>
                  logger.info(s"Master $masterIP is ALIVE, triggering a kill through UI")
                  val url = s"${baseSparkURL}/app/kill/"
                  val data = FormData("id" -> appId, "terminate" -> "true")
                  val killAppRequest = Post(url, data)
                  getHTTPResponse(killAppRequest).map {
                    _ =>
                      logger.info(s"Successfully killed the app $appId through spark master $masterIP")
                      system.terminate()
                      return
                  }
                case false =>
                  logger.warn(s"The status of spark master at $masterIP is $status. Skipping kill on it.")
              }
            })
        }
    }
    logger.error("No master found in ACTIVE or alive state. Throwing exception.")
    throw new NoAliveMasterException()
  }

  protected def getHTTPResponse(req: HttpRequest)(implicit system: ActorSystem): Option[HttpResponse] = {
    val result = Try {
      doRequest(req)
    }
    result match {
      case Success(response) if response.status.isSuccess => return Some(response)
      case Success(response) if response.status.isFailure =>
        logger.error(s"Failed to complete request (${req.uri})." +
          s"Status code ${response.status.intValue}, complete response is $response")
        return None
      case Failure(e) =>
        logger.error(s"Request ${req.uri} failed. Complete request ${req}", e)
        return None
    }
  }

  protected def doRequest(req: HttpRequest)(implicit system: ActorSystem): HttpResponse = {
    import scala.concurrent.duration._
    val response: Future[HttpResponse] = Http().singleRequest(req)
    Await.result(response, 3 seconds)
  }
}