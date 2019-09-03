package spark.jobserver.integrationtests.util

import com.softwaremill.sttp._
import play.api.libs.json.Json

object TestHelper {

  def fileToByteArray(fileName : String) : Array[Byte] = {
    try{
      val stream = getClass().getResourceAsStream(s"/$fileName")
      Iterator continually stream.read takeWhile (-1 !=) map (_.toByte) toArray
    } catch {
      case e: Exception =>
        println(s"Could not open $fileName.")
        e.printStackTrace()
        sys.exit(-1)
    }
  }

  def waitForContextTermination(sjs: String, contextName: String, retries: Int){
    implicit val backend = HttpURLConnectionBackend()
    val SLEEP_BETWEEN_RETRIES = 1000;
    val request = sttp.get(uri"$sjs/contexts/$contextName")
    var response = request.send()
    var json = Json.parse(response.body.merge)
    var state = (json \ "state").as[String]
    var retriesLeft = retries
    while(state != "FINISHED" && retriesLeft > 0){
      Thread.sleep(SLEEP_BETWEEN_RETRIES)
      response = request.send()
      json = Json.parse(response.body.merge)
      state = (json \ "state").as[String]
      retriesLeft -= 1
    }
  }

  def waitForJobTermination(sjs: String, jobId: String, retries: Int){
    implicit val backend = HttpURLConnectionBackend()
    val request = sttp.get(uri"$sjs/jobs/$jobId")
    var response = request.send()
    var json = Json.parse(response.body.merge)
    var state = (json \ "status").as[String]
    var retriesLeft = retries
    while(state != "FINISHED" && retriesLeft > 0){
      Thread.sleep(1000)
      response = request.send()
      json = Json.parse(response.body.merge)
      state = (json \ "status").as[String]
      retriesLeft -= 1
    }
  }

}