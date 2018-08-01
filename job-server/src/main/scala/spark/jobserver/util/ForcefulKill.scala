package spark.jobserver.util

import com.typesafe.config.Config
import spray.client.pipelining._
import spray.http.{HttpResponse, HttpRequest, FormData}
import spray.json._
import scala.concurrent.{Await, Future}
import akka.actor._

trait ForcefulKill {
  def kill(): Unit
}

class StandaloneForcefulKill(config: Config, appId: String) extends ForcefulKill {
  override def kill(): Unit = {
    implicit val system = ActorSystem()
    import system.dispatcher
    import scala.concurrent.duration._
    val pipeline = sendReceive
    val sparkMasterString = config.getString("spark.master")
    val mode = sparkMasterString.split("//")
    if (!mode.head.containsSlice("spark")) throw new NotStandaloneModeException()
    val tempSplit = mode.drop(1).head.split(",")
    var sparkMasterArray = scala.collection.mutable.ArrayBuffer.empty[String]
    for (i <- tempSplit) {
      val temp = i.split(":").filterNot(_.trim.equals(""))
      sparkMasterArray += temp.head
    }
    var checkStatus = false

    while(!checkStatus && !sparkMasterArray.isEmpty) {
      val request = Get("http://" + sparkMasterArray.head + ":8080/json/")
      val result: HttpResponse = getHTTPResponse(pipeline, request)
      val ent = result.entity.data.asString
      //  "status" is showing the state of this master
      val status = ent.parseJson.asJsObject.getFields("status").mkString
      checkStatus = status.contains("ALIVE")
      if (!checkStatus) sparkMasterArray = sparkMasterArray.drop(1)
      if (sparkMasterArray.isEmpty) throw new NoAliveMasterException()
    }

    val url = "http://" + sparkMasterArray.head + ":8080/app/kill/"
    val data = FormData(Seq("id" -> appId, "terminate" -> "true"))
    val request = Post(url, data)
    val result = getHTTPResponse(pipeline, request)
    system.terminate()
  }

  protected def getHTTPResponse(pipeline: SendReceive, req: HttpRequest): HttpResponse = {
    import scala.concurrent.duration._
    val responce: Future[HttpResponse] = pipeline(req)
    val result = Await.result(responce, 3 seconds)
    result
  }
}