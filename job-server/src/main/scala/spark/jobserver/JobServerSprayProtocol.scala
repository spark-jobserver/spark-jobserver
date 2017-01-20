package spark.jobserver

import spray.json.DefaultJsonProtocol
import spray.json._

/**
  * Created by alexsilva on 8/20/16.
  */
case class JobServerResponse(status: String, result: String) {
  def isSuccess:Boolean = status == "SUCCESS"
}

object JobServerSprayProtocol extends DefaultJsonProtocol {
  implicit val jobServerResponseFormat: RootJsonFormat[JobServerResponse] = jsonFormat2(JobServerResponse)

}
