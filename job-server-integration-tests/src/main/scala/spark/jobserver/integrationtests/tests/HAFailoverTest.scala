package spark.jobserver.integrationtests.tests

import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import com.softwaremill.sttp._
import com.typesafe.config.Config

import play.api.libs.json.Json
import spark.jobserver.integrationtests.util.DeploymentController
import spark.jobserver.integrationtests.util.TestHelper

class HAFailoverTest extends FreeSpec with Matchers with BeforeAndAfterAllConfigMap {

  // Configuration
  var SJS1 = ""
  var SJS2 = ""
  implicit val backend = HttpURLConnectionBackend()
  var controller : DeploymentController = _

  override def beforeAll(configMap: ConfigMap) = {
    val config = configMap.getRequired[Config]("config")
    val jobservers = config.getStringList("jobserverAddresses")
    if(jobservers.size() < 2){
      println("You need to specify two jobserver addresses in the config to run HA tests.")
      sys.exit(-1)
    }
    SJS1 = jobservers.get(0)
    SJS2 = jobservers.get(1)
    controller = DeploymentController.fromConfig(config)
    // Restart second jobserver to make sure the first one is the singleton
    controller.isJobserverUp(SJS2) should equal(true)
    controller.stopJobserver(SJS2)
    controller.isJobserverUp(SJS2) should equal(false)
    controller.startJobserver(SJS2)
    controller.isJobserverUp(SJS2) should equal(true)
  }

  // Test artifacts
  val contextName = "HAFailoverIntegrationTestContext"

  "Sustain one jobserver failure" - {

    "Stopping Jobserver 1 should succeed" in {
      controller.isJobserverUp(SJS1) should equal(true)
      controller.stopJobserver(SJS1)
      controller.isJobserverUp(SJS1) should equal(false)
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
      TestHelper.waitForContextTermination(SJS2, contextName, 3)
      val request2 = sttp.get(uri"$SJS2/contexts/$contextName")
      val response2 = request2.send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "state").as[String] should equal("FINISHED")
    }

    "Restart of Jobserver 1 should succeed" in {
      controller.isJobserverUp(SJS1) should equal(false)
      controller.startJobserver(SJS1)
      controller.isJobserverUp(SJS1) should equal(true)
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
      TestHelper.waitForContextTermination(SJS1, contextName, 3)
      val request2 = sttp.get(uri"$SJS1/contexts/$contextName")
      val response2 = request2.send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "state").as[String] should equal("FINISHED")
    }

  }

  override def afterAll(configMap: ConfigMap) = {
    // Clean up used contexts
    sttp.delete(uri"$SJS1/binaries/$contextName")
  }

}
