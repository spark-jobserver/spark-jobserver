package spark.jobserver.integrationtests.tests

import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.FreeSpec
import org.scalatest.Matchers
import com.softwaremill.sttp._
import play.api.libs.json.Json
import play.api.libs.json.JsValue.jsValueToJsLookup
import scala.util.Either.MergeableEither

class TwoJobserverTests extends FreeSpec with Matchers with BeforeAndAfterAllConfigMap {

  // Configuration
  var SJS1 = ""
  var SJS2 = ""
  implicit val backend = HttpURLConnectionBackend()
  override def beforeAll(configMap: ConfigMap) = {
    SJS1 = configMap.getWithDefault("sjs1", "localhost:8090")
    SJS2 = configMap.getWithDefault("sjs2", "localhost:8091")
  }

  "Synchronize contexts across jobservers" - {

    val context1 = "HAIntegrationTestContext1"
    val context2 = "HAIntegrationTestContext2"

    "POST /context on sjs1 should be visible on sjs2" in {
      // Create on SJS1
      val response1 = sttp.post(uri"$SJS1/contexts/$context1").send()
      response1.code should equal(200)
      val json1 = Json.parse(response1.body.merge)
      (json1 \ "status").as[String] should equal("SUCCESS")
      // Read from SJS2
      val response2 = sttp.get(uri"$SJS2/contexts/$context1").send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "name").as[String] should equal(context1)
      (json2 \ "state").as[String] should equal("RUNNING")
    }

    "POST /context on sjs2 should be visible on sjs1" in {
      // Create on SJS2
      val response1 = sttp.post(uri"$SJS2/contexts/$context2").send()
      response1.code should equal(200)
      val json1 = Json.parse(response1.body.merge)
      (json1 \ "status").as[String] should equal("SUCCESS")
      // Read from SJS1
      val response2 = sttp.get(uri"$SJS1/contexts/$context2").send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "name").as[String] should equal(context2)
      (json2 \ "state").as[String] should equal("RUNNING")
    }

    "DELETE /context on sjs1 should be visible on sjs2" in {
      // DELETE on SJS1
      val response1 = sttp.delete(uri"$SJS1/contexts/$context2").send()
      response1.code should equal(200)
      val json1 = Json.parse(response1.body.merge)
      (json1 \ "status").as[String] should equal("SUCCESS")
      // Read from SJS2
      val response2 = sttp.get(uri"$SJS2/contexts/$context2").send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "name").as[String] should equal(context2)
      (json2 \ "state").as[String] should equal("FINISHED")
    }

    "DELETE /context on sjs2 should be visible on sjs1" in {
      // DELETE on SJS2
      val response1 = sttp.delete(uri"$SJS2/contexts/$context1").send()
      response1.code should equal(200)
      val json1 = Json.parse(response1.body.merge)
      (json1 \ "status").as[String] should equal("SUCCESS")
      // Read from SJS1
      val response2 = sttp.get(uri"$SJS1/contexts/$context1").send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "name").as[String] should equal(context1)
      (json2 \ "state").as[String] should equal("FINISHED")
    }

  }

}