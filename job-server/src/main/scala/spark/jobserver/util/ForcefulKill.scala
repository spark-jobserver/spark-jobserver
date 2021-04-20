package spark.jobserver.util

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.{Get, Post}
import akka.http.scaladsl.model.{FormData, HttpRequest, HttpResponse}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

trait ForcefulKill {
  def kill()(implicit system: ActorSystem): Unit
}

class StandaloneForcefulKill(config: Config, appId: String) extends ForcefulKill {
  private val logger = LoggerFactory.getLogger(getClass)
  private val sparkUIRequestTimeout = 3.seconds

  override def kill()(implicit system: ActorSystem): Unit = {
    val sparkMasterString = config.getString("spark.master")
    val mode = sparkMasterString.split("//")
    if (!mode.head.containsSlice("spark")) throw new NotStandaloneModeException()

    val mastersIPsWithPort = mode.drop(1).head.split(",")
    val masterIPs = mastersIPsWithPort.map(_.split(":").filterNot(_.trim().equals("")).head)

    implicit val ec = system.dispatcher

    masterIPs.foreach {
      masterIP =>
        val baseSparkURL = s"http://$masterIP:8080"
        val getJsonRequest = Get(s"${baseSparkURL}/json/")

        getHTTPResponse(getJsonRequest).flatMap(getFullResponseBody(_)).map {
          responseBody =>
            val status = responseBody.parseJson.asJsObject.getFields("status").mkString
            status.contains("ALIVE") match {
              case true =>
                logger.info(s"Master $masterIP is ALIVE, triggering a kill through UI")
                val url = s"${baseSparkURL}/app/kill/"
                val data = FormData("id" -> appId, "terminate" -> "true")
                val killAppRequest = Post(url, data)
                getHTTPResponse(killAppRequest).map {
                  _ =>
                    logger.info(s"Successfully killed the app $appId through spark master $masterIP")
                    return
                }
              case false =>
                logger.warn(s"The status of spark master at $masterIP is $status. Skipping kill on it.")
            }
        }
    }
    logger.error("No master found in ACTIVE or alive state. Throwing exception.")
    throw NoAliveMasterException()
  }

  private def getFullResponseBody(httpResponseEntity: HttpResponse)(
    implicit system: ActorSystem): Option[String] = {
    implicit val ec: ExecutionContext = system.dispatcher
    Try {
      Await.result(httpResponseEntity.entity.toStrict(sparkUIRequestTimeout).map(_.data.utf8String),
        sparkUIRequestTimeout + 1.second)
    } match {
      case Success(content) => Some(content)
      case Failure(e) =>
        logger.error(s"Failed to retrieve full response from Spark UI. " +
          s"Complete response ${httpResponseEntity}", e)
        None
    }
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
        None
      case Failure(e) =>
        logger.error(s"Request ${req.uri} failed. Complete request ${req}", e)
        None
    }
  }

  protected def doRequest(req: HttpRequest)(implicit system: ActorSystem): HttpResponse = {
    val response: Future[HttpResponse] = Http().singleRequest(req)
    Await.result(response, sparkUIRequestTimeout)
  }
}