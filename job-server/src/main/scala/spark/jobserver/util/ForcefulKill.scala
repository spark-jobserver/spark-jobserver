package spark.jobserver.util

import com.typesafe.config.Config
import akka.http.scaladsl.{Http, model}
import akka.http.scaladsl.model.Multipart.FormData
import spray.json._

import scala.concurrent.{Await, Future}
import akka.actor._
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import javax.ws.rs.GET

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

      val fBody = Await.result(Http().singleRequest(getRequest).map {
        case HttpResponse(StatusCodes.OK, headers, entity, _) => entity.toStrict(3 seconds).map(_.data).map(_.utf8String)
        case HttpResponse(status, headers, entity, _) => throw new Exception(s"Failed to get response: [$status] with body: $entity")
      },5 seconds)
      val ent = Await.result(fBody, 5 seconds)
//      val result: HttpResponse = getHTTPResponse(pipeline, request)
      //  "status" is showing the state of this master
      val status = ent.parseJson.asJsObject.getFields("status").mkString
      checkStatus = status.contains("ALIVE")
      if (!checkStatus) sparkMasterArray = sparkMasterArray.drop(1)
      if (sparkMasterArray.isEmpty) throw NoAliveMasterException()
    }

    val data = Multipart.FormData(Map(
      "id" -> HttpEntity.Strict(MediaTypes.`text/plain`.toContentTypeWithMissingCharset, ByteString(appId)),
      "terminate" -> HttpEntity.Strict(MediaTypes.`text/plain`.toContentTypeWithMissingCharset, ByteString("true"))
    )
    ).toEntity()

    val postRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri("http://" + sparkMasterArray.head + ":8080/app/kill/"),
      entity = data
    )

   Http().singleRequest(postRequest).map {
      case HttpResponse(status, headers, entity, _) =>
    }

    system.terminate()
  }
}