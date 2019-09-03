package spark.jobserver.integrationtests.tests

import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import com.softwaremill.sttp._

import play.api.libs.json.Json
import spark.jobserver.integrationtests.util.TestHelper

class CornerCasesTests extends FreeSpec with Matchers with BeforeAndAfterAllConfigMap {

  // Configuration
  var SJS = ""
  implicit val backend = HttpURLConnectionBackend()

  override def beforeAll(configMap: ConfigMap) = {
    SJS = configMap.getWithDefault("address", "localhost:8090")
  }

  // Test environment
  val bin = "tests.jar"
  val streamingbin = "BasicTest.jar"
  val deletionTestApp = "IntegrationTestDeletionTest"

  "DELETE /binaries should not delete binaries with running jobs" in {
    var jobId : String = ""

    // upload binary
    val byteArray = TestHelper.fileToByteArray(bin)
    val response1 = sttp.post(uri"$SJS/binaries/$deletionTestApp")
        .body(byteArray)
        .contentType("application/java-archive")
        .send()
    response1.code should equal(200)

    // submit long running job
    val response2 = sttp.post(uri"$SJS/jobs?appName=$deletionTestApp&classPath=spark.jobserver.LongPiJob")
        .body("stress.test.longpijob.duration = 10")
        .send()
    response2.code should equal(202)
    val json2 = Json.parse(response2.body.merge)
    (json2 \ "status").as[String] should equal("STARTED")
    jobId = (json2 \ "jobId").as[String]

    // try to delete binary
    val response3 = sttp.delete(uri"$SJS/binaries/$deletionTestApp")
        .send()
    response3.code should equal(403)
    val json3 = Json.parse(response3.body.merge)
    val message = (json3 \ "result").as[String]
    message should include("is in use")
    message should include(jobId)

    // wait for job termination
    TestHelper.waitForJobTermination(SJS, jobId, 15)
    val request = sttp.get(uri"$SJS/jobs/$jobId")
    val response = request.send()
    val json = Json.parse(response.body.merge)
    (json \ "status").as[String] should equal("FINISHED")

    // deletion should succeed finally
    val response4 = sttp.delete(uri"$SJS/binaries/$deletionTestApp")
        .send()
    response4.code should equal(200)
  }

  override def afterAll(configMap: ConfigMap) = {
    // Clean up test entities just in case something went wrong
    sttp.delete(uri"$SJS/binaries/$deletionTestApp")
  }

}
