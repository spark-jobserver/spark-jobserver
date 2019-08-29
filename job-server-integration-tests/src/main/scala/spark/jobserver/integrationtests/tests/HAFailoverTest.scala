package spark.jobserver.integrationtests.tests

import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import com.softwaremill.sttp._

import play.api.libs.json.Json
import spark.jobserver.integrationtests.util.BoshController

class HAFailoverTest extends FreeSpec with Matchers with BeforeAndAfterAllConfigMap {

  // Configuration
  var SJS1 = ""
  var SJS2 = ""
  implicit val backend = HttpURLConnectionBackend()
  override def beforeAll(configMap: ConfigMap) = {
    SJS1 = configMap.getWithDefault("sjs1", "localhost:8090")
    SJS2 = configMap.getWithDefault("sjs2", "localhost:8091")
    // Restart second jobserver to make sure the first one is the singleton
    val controller = new BoshController()
    val ip2 = SJS2.split(":").head
    controller.isJobserverUp(ip2) should equal(true)
    controller.stopJobserver(ip2)
    controller.isJobserverUp(ip2) should equal(false)
    controller.startJobserver(ip2)
    controller.isJobserverUp(ip2) should equal(true)
  }

  // Test artifacts
  val contextName = "HAFailoverIntegrationTestContext"

  "Sustain one jobserver failure" - {

    "Stopping Jobserver 1 should succeed" in {
      val controller = new BoshController()
      val ip1 = SJS1.split(":").head
      controller.isJobserverUp(ip1) should equal(true)
      controller.stopJobserver(ip1)
      controller.isJobserverUp(ip1) should equal(false)
    }

    // sample test representing every API call that is not routed through ClusterSupervisor
    "GET /binaries should still work on Jobserver 2" in {
      val request = sttp.get(uri"$SJS2/binaries")
      val response = request.send()
      response.code should equal(200)
    }

    "waiting for 20s until the cluster has fully recovered..." in {
      Thread.sleep(20000)
    }

    "POST /contexts should still work on Jobserver 2" in {
      val request = sttp.post(uri"$SJS2/contexts/$contextName")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "status").as[String] should equal("SUCCESS")
    }

    "GET /contexts should still work on Jobserver 2" in {
      val request = sttp.get(uri"$SJS2/contexts")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      val allContexts = json.as[List[String]]
      allContexts.contains(contextName) should equal(true)
    }

    "DELETE /contexts should still work on Jobserver 2" in {
      val request = sttp.delete(uri"$SJS2/contexts/$contextName")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "status").as[String] should equal("SUCCESS")
      // status finished?
      val request2 = sttp.get(uri"$SJS2/contexts/$contextName")
      var response2 = request2.send()
      response2.code should equal(200)
      var json2 = Json.parse(response2.body.merge)
      var state = (json2 \ "state").as[String]
      // Stopping may take some time, so retry in such case
      var retries = 3
      while(state == "STOPPING" && retries > 0){
        Thread.sleep(1000)
        response2 = request2.send()
        response2.code should equal(200)
        json2 = Json.parse(response2.body.merge)
        state = (json2 \ "state").as[String]
        retries -= 1
      }
      state should equal("FINISHED")
    }

    "Restart of Jobserver 1 should succeed" in {
      val controller = new BoshController()
      val ip1 = SJS1.split(":").head
      controller.isJobserverUp(ip1) should equal(false)
      controller.startJobserver(ip1)
      controller.isJobserverUp(ip1) should equal(true)
    }

    "GET /binaries should work again on Jobserver 1" in {
      val request = sttp.get(uri"$SJS1/binaries")
      val response = request.send()
      response.code should equal(200)
    }

    "POST /contexts should work again on Jobserver 1" in {
      val request = sttp.post(uri"$SJS1/contexts/$contextName")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "status").as[String] should equal("SUCCESS")
    }

    "GET /contexts should work again on Jobserver 1" in {
      val request = sttp.get(uri"$SJS1/contexts")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      val allContexts = json.as[List[String]]
      allContexts.contains(contextName) should equal(true)
    }

    "DELETE /contexts should work again on Jobserver 1" in {
      val request = sttp.delete(uri"$SJS1/contexts/$contextName")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "status").as[String] should equal("SUCCESS")
      // status finished?
      val request2 = sttp.get(uri"$SJS1/contexts/$contextName")
      var response2 = request2.send()
      response2.code should equal(200)
      var json2 = Json.parse(response2.body.merge)
      var state = (json2 \ "state").as[String]
      // Stopping may take some time, so retry in such case
      var retries = 3
      while(state == "STOPPING" && retries > 0){
        Thread.sleep(1000)
        response2 = request2.send()
        response2.code should equal(200)
        json2 = Json.parse(response2.body.merge)
        state = (json2 \ "state").as[String]
        retries -= 1
      }
      state should equal("FINISHED")
    }

  }

  override def afterAll(configMap: ConfigMap) = {
    // Clean up used contexts
    sttp.delete(uri"$SJS1/binaries/$contextName")
  }

}