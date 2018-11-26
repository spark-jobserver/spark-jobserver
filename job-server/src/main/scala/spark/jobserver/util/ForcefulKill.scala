package spark.jobserver.util

import com.typesafe.config.Config
import akka.http.scaladsl.{Http}
import akka.http.scaladsl.model.FormData
import spray.json._

import scala.concurrent.{Await, Future}
import akka.actor._
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import javax.ws.rs.GET

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

trait ForcefulKill {
  def kill(): Unit
}

class StandaloneForcefulKill(config: Config, appId: String) extends ForcefulKill {
  override def kill(): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))
    import system.dispatcher
    import scala.concurrent.duration._
    val sparkMasterString = config.getString("spark.master")
    val mode = sparkMasterString.split("//")
    if (!mode.head.containsSlice("spark")) throw NotStandaloneModeException()
    val tempSplit = mode.drop(1).head.split(",")
    var sparkMasterArray = scala.collection.mutable.ArrayBuffer.empty[String]
    for (i <- tempSplit) {
      val temp = i.split(":").filterNot(_.trim == "")
      sparkMasterArray += temp.head
    }
    var checkStatus = false

    while(!checkStatus && sparkMasterArray.nonEmpty) {

      val getRequest = HttpRequest(
        method = HttpMethods.GET,
        uri = Uri("http://" + sparkMasterArray.head + ":8080/json/")
      )

      val fBody = getHTTPResponse(getRequest).entity.toStrict(30 seconds).map(_.data).map(_.utf8String)

      val ent = Await.result(fBody, 5 seconds)
//      val result: HttpResponse = getHTTPResponse(pipeline, request)
      //  "status" is showing the state of this master
      val status = ent.parseJson.asJsObject.getFields("status").mkString
      checkStatus = status.contains("ALIVE")
      if (!checkStatus) sparkMasterArray = sparkMasterArray.drop(1)
      if (sparkMasterArray.isEmpty) throw NoAliveMasterException()
    }

    val data = FormData(Map(
      "id" -> appId,
      "terminate" -> "true"
     )
    )

    val postRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri("http://" + sparkMasterArray.head + ":8080/app/kill/"),
      entity = data.toEntity
    )

   getHTTPResponse(postRequest)

    system.terminate()
  }

    import scala.concurrent.duration._
  def getHTTPResponse(req: HttpRequest, timout: Duration = 30 seconds)
                     (implicit system: ActorSystem): HttpResponse = {
    Await.result(Http().singleRequest(req), timout)
  }

}