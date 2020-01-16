package spark.jobserver.integrationtests.tests

import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import com.softwaremill.sttp._
import com.typesafe.config.Config

import play.api.libs.json.Json
import spark.jobserver.integrationtests.util.TestHelper

class TwoJobserverTests extends FreeSpec with Matchers with BeforeAndAfterAllConfigMap {

  // Configuration
  var SJS1 = ""
  var SJS2 = ""
  implicit val backend = HttpURLConnectionBackend()

  override def beforeAll(configMap: ConfigMap): Unit = {
    val config = configMap.getRequired[Config]("config")
    val jobservers = config.getStringList("jobserverAddresses")
    if (jobservers.size() < 2) {
      println("You need to specify two jobserver addresses in the config to run HA tests.")
      sys.exit(-1)
    }
    SJS1 = jobservers.get(0)
    SJS2 = jobservers.get(1)
  }

  // Test artifacts
  val context1 = "HAIntegrationTestContext1"
  val context2 = "HAIntegrationTestContext2"

  "Synchronize contexts across jobservers" - {

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
      TestHelper.waitForContextTermination(SJS2, context2)
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
      TestHelper.waitForContextTermination(SJS1, context1)
      val response2 = sttp.get(uri"$SJS1/contexts/$context1").send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "name").as[String] should equal(context1)
      (json2 \ "state").as[String] should equal("FINISHED")
    }

  }

  override def afterAll(configMap: ConfigMap): Unit = {
    sttp.delete(uri"$SJS1/contexts/$context1")
    sttp.delete(uri"$SJS1/contexts/$context2")
  }

}