package spark.jobserver.integrationtests.tests

import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import com.softwaremill.sttp._

import play.api.libs.json.JsObject
import play.api.libs.json.Json
import spark.jobserver.integrationtests.util.TestHelper

class BasicApiTests extends FreeSpec with Matchers with BeforeAndAfterAllConfigMap{

  // Configuration
  var SJS = ""
  implicit val backend = HttpURLConnectionBackend()

  override def beforeAll(configMap: ConfigMap) = {
    SJS = configMap.getWithDefault("address", "localhost:8090")
  }

  // Test environment
  val bin = "tests.jar"
  val streamingbin = "BasicTest.jar"
  val appName = "IntegrationTestApp"
  val contextName = "IntegrationTestContext"
  val app = "IntegrationTestTestsJar"
  val streamingApp = "IntegrationTestStreamingApp"
  val batchContextName = "IntegrationTestBatchContext"
  val streamingContextName = "IntegrationTestStreamingContext"

  "/binaries" - {
    var binaryUploadDate : DateTime = null

    "POST /binaries/<app> should upload a binary" in {
      val byteArray = TestHelper.fileToByteArray(bin)

      val request = sttp.post(uri"$SJS/binaries/$appName")
          .body(byteArray)
          .contentType("application/java-archive")
      val response = request.send()
      response.code should equal(200)
    }

    "GET /binaries should list all available binaries" in {
      val request = sttp.get(uri"$SJS/binaries")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      val testbin = (json \ appName)
      testbin.isDefined should equal(true)
      binaryUploadDate = new DateTime((testbin \ "upload-time").as[String])
    }

    "POST /binaries/<app> should overwrite a binary with a new version" in {
      val byteArray = TestHelper.fileToByteArray(streamingbin)
      val request = sttp.post(uri"$SJS/binaries/$appName")
          .body(byteArray)
          .contentType("application/java-archive")
      val response = request.send()
      response.code should equal(200)
      // See if date has been updated
      val getrequest = sttp.get(uri"$SJS/binaries")
      val getresponse = getrequest.send()
      getresponse.code should equal(200)
      val json = Json.parse(getresponse.body.merge)
      val testbin = (json \ appName)
      testbin.isDefined should equal(true)
      val newUploadDate = new DateTime((testbin \ "upload-time").as[String])
      newUploadDate.isAfter(binaryUploadDate) should equal(true)
    }

    "DELETE /binaries/<app> should delete all binary versions under this name" in {
      val request = sttp.delete(uri"$SJS/binaries/$appName")
      val response = request.send()
      response.code should equal(200)
      // See if not listed anymore
      val getrequest = sttp.get(uri"$SJS/binaries")
      val getresponse = getrequest.send()
      getresponse.code should equal(200)
      val json = Json.parse(getresponse.body.merge)
      val testbin = (json \ appName)
      testbin.isDefined should equal(false)
    }

    "Error scenarios" - {
      "DELETE /binaries<app> should fail if the binary does not exist" in {
        val request = sttp.delete(uri"$SJS/binaries/$appName")
        val response = request.send()
        response.code should equal(404)
      }
    }

  }

  "/contexts" - {

    "POST /contexts/<contextName> should create a new context" in {
      val request = sttp.post(uri"$SJS/contexts/$contextName")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "status").as[String] should equal("SUCCESS")
    }

    "GET /contexts should list all contexts" in {
      val request = sttp.get(uri"$SJS/contexts")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      val allContexts = json.as[List[String]]
      allContexts.contains(contextName) should equal(true)
    }

    "GET /contexts/<contextName> should retrieve infos about a context" in {
      val request = sttp.get(uri"$SJS/contexts/$contextName")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "name").as[String] should equal(contextName)
      (json \ "state").as[String] should equal("RUNNING")
    }

    "DELETE /contexts/<contextName> should delete a context" in {
      val request = sttp.delete(uri"$SJS/contexts/$contextName")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "status").as[String] should equal("SUCCESS")
      // state finished?
      TestHelper.waitForContextTermination(SJS, contextName, 5)
      val request2 = sttp.get(uri"$SJS/contexts/$contextName")
      val response2 = request2.send()
      response2.code should equal(200)
      val json2 = Json.parse(response2.body.merge)
      (json2 \ "state").as[String] should equal("FINISHED")
    }

    "GET /contexts should not list deleted contexts" in {
      val request = sttp.get(uri"$SJS/contexts")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      val allContexts = json.as[List[String]]
      allContexts.contains(contextName) should equal(false)
    }

    "Error scenarios" - {
      "POST /contexts/<contextName> should fail if the context name already exists" in {
        // Initial POST
        val request = sttp.post(uri"$SJS/contexts/$contextName")
        val response = request.send()
        response.code should equal(200)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("SUCCESS")
        // Second POST
        val response2 = request.send()
        response2.code should equal(400)
        val json2 = Json.parse(response2.body.merge)
        (json2 \ "status").as[String] should equal("ERROR")
        // Clean up again
        sttp.delete(uri"$SJS/contexts/$contextName").send()
      }

      "DELETE /contexts/<contextName> should fail if there is no such context" in {
        val request = sttp.delete(uri"$SJS/contexts/$contextName")
        val response = request.send()
        response.code should equal(404)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("ERROR")
      }
    }

  }

  "/jobs" - {

    var adHocJobId : String = ""
    var batchJobId: String = ""
    var streamingJobId : String = ""
    var jobContext : String = ""

    "(preparation) uploading job binaries for job testing should not fail" in {
      val byteArray1 = TestHelper.fileToByteArray(bin)
      val byteArray2 = TestHelper.fileToByteArray(streamingbin)
      sttp.post(uri"$SJS/binaries/$app")
          .body(byteArray1)
          .contentType("application/java-archive")
          .send()
          .code should equal(200)
      sttp.post(uri"$SJS/binaries/$streamingApp")
          .body(byteArray2)
          .contentType("application/java-archive")
          .send().
          code should equal(200)
      val response = sttp.get(uri"$SJS/binaries").send()
      response.code should equal(200)
      val binaries = Json.parse(response.body.merge)
      (binaries \ app).isDefined should equal(true)
      (binaries \ streamingApp).isDefined should equal(true)
    }

    "adHoc jobs" - {
      "POST /jobs should start a job in adHoc context" in {
        val request = sttp.post(uri"$SJS/jobs?appName=$app&classPath=spark.jobserver.WordCountExample")
            .body("input.string = a b c a b see")
        val response = request.send()
        response.code should equal(202)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("STARTED")
        jobContext = (json \ "context").as[String]
        jobContext should include("WordCountExample")
        adHocJobId = (json \ "jobId").as[String]
      }

      "the termination of the job should also terminate the adHoc context" in {
        // Context finished?
        TestHelper.waitForContextTermination(SJS, jobContext, 5)
        val request = sttp.get(uri"$SJS/contexts/$jobContext")
        val response = request.send()
        response.code should equal(200)
        val json = Json.parse(response.body.merge)
        (json \ "state").as[String] should equal("FINISHED")
        // Job finished?
        val jobRequest = sttp.get(uri"$SJS/jobs/$adHocJobId")
        val jobResponse = jobRequest.send()
        jobResponse.code should equal(200)
        val jobJson = Json.parse(jobResponse.body.merge)
        (jobJson \ "status").as[String] should equal("FINISHED")
      }
    }

    "batch jobs" - {
      "POST /jobs?context=<context> should start a job in an existing (batch) context" in {
        // Start context
        jobContext = batchContextName
        val contextRequest = sttp.post(uri"$SJS/contexts/$jobContext")
        val contextResponse = contextRequest.send()
        contextResponse.code should equal(200)
        val contextJson = Json.parse(contextResponse.body.merge)
        (contextJson \ "status").as[String] should equal("SUCCESS")
        // Start job
        val request = sttp.post(uri"$SJS/jobs?appName=$app&classPath=spark.jobserver.WordCountExample&context=$jobContext")
            .body("input.string = a b c a b see")
        val response = request.send()
        response.code should equal(202)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("STARTED")
        (json \ "context").as[String] should equal(jobContext)
        batchJobId = (json \ "jobId").as[String]
      }

      "the termination of the job should not terminate the context" in {
        Thread.sleep(10000)
        // Job finished?
        val jobRequest = sttp.get(uri"$SJS/jobs/$batchJobId")
        val jobResponse = jobRequest.send()
        jobResponse.code should equal(200)
        val jobJson = Json.parse(jobResponse.body.merge)
        (jobJson \ "status").as[String] should equal("FINISHED")
        // Context running?
        val contextRequest = sttp.get(uri"$SJS/contexts/$jobContext")
        val contextResponse = contextRequest.send()
        contextResponse.code should equal(200)
        val contextJson = Json.parse(contextResponse.body.merge)
        (contextJson \ "state").as[String] should equal("RUNNING")
        // Cleanup
        val deleteResponse = sttp.delete(uri"$SJS/contexts/$jobContext").send()
        deleteResponse.code should equal(200)
      }
    }

    "streaming jobs" - {
      "POST /jobs?context=<streamingContext> should start a job in an existing (streaming) context" in {
        // Start context
        jobContext = batchContextName
        val contextRequest = sttp.post(uri"$SJS/contexts/$jobContext?context-factory=spark.jobserver.context.StreamingContextFactory")
        val contextResponse = contextRequest.send()
        contextResponse.code should equal(200)
        val contextJson = Json.parse(contextResponse.body.merge)
        (contextJson \ "status").as[String] should equal("SUCCESS")
        // Start job
        val request = sttp.post(uri"$SJS/jobs?appName=$streamingApp&classPath=com.sap.hcpbd.Streaming&context=$jobContext")
        val response = request.send()
        response.code should equal(202)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("STARTED")
        (json \ "context").as[String] should equal(jobContext)
        streamingJobId = (json \ "jobId").as[String]
      }

      "DELETE /jobs/<jobId> should stop a streaming job" in {
        val request = sttp.delete(uri"$SJS/jobs/$streamingJobId")
        val response = request.send()
        response.code should equal(200)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("KILLED")
      }

      "the termination of the streaming job should not terminate the streaming context" in {
        Thread.sleep(10000)
        // Job in state killed?
        val requestJob = sttp.get(uri"$SJS/jobs/$streamingJobId")
        var response = requestJob.send()
        response.code should equal(200)
        var json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("KILLED")
        // Context running?
        val contextRequest = sttp.get(uri"$SJS/contexts/$jobContext")
        response = contextRequest.send()
        response.code should equal(200)
        json = Json.parse(response.body.merge)
        (json \ "state").as[String] should equal("RUNNING")
        // Cleanup
        response = sttp.delete(uri"$SJS/contexts/$jobContext").send()
        response.code should equal(200)
      }
    }

    "GET /jobs should list all jobs" in {
      val request = sttp.get(uri"$SJS/jobs")
      val response = request.send()
      response.code should equal(200)
      val allJobs = Json.parse(response.body.merge).as[List[JsObject]]
      val jobCount = allJobs.length
      jobCount should be >= 3
      allJobs.exists(o => (o \ "jobId").as[String] == adHocJobId) should equal(true)
      allJobs.exists(o => (o \ "jobId").as[String] == batchJobId) should equal(true)
      allJobs.exists(o => (o \ "jobId").as[String] == streamingJobId) should equal(true)
    }

    "GET /jobs/<id> should show job information" in {
      val request = sttp.get(uri"$SJS/jobs/$streamingJobId")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      (json \ "jobId").as[String] should equal(streamingJobId)
    }

    "GET /jobs/<id>/config should return the job config" in {
      val request = sttp.get(uri"$SJS/jobs/$batchJobId/config")
      val response = request.send()
      response.code should equal(200)
      val json = Json.parse(response.body.merge)
      ((json \ "input") \ "string").as[String] should equal("a b c a b see")
    }

    "Error scenarios" - {

      "POST /jobs should fail if there is no such app" in {
        val request = sttp.post(uri"$SJS/jobs?appName=NonExistingAppName&classPath=spark.jobserver.WordCountExample")
        val response = request.send()
        response.code should equal(404)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("ERROR")
        (json \ "result").as[String] should include("appName")
        (json \ "result").as[String] should include("not found")
      }

      "POST /jobs should fail if there is no such context" in {
        val request = sttp.post(uri"$SJS/jobs?appName=$streamingApp&classPath=com.sap.hcpbd.Streaming&context=NonExistingContext")
        val response = request.send()
        response.code should equal(404)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("ERROR")
        (json \ "result").as[String] should include("context")
        (json \ "result").as[String] should include("not found")
      }

      "GET /jobs/<id> should return an error if there is no such id" in {
        val request = sttp.get(uri"$SJS/jobs/NonExistingId")
        val response = request.send()
        response.code should equal(404)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("ERROR")
        (json \ "result").as[String] should include("No such job ID")
      }

      "GET /jobs/<id>/config should return an error if there is no such id" in {
        val request = sttp.get(uri"$SJS/jobs/NonExistingId/config")
        val response = request.send()
        response.code should equal(404)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("ERROR")
        (json \ "result").as[String] should include("No such job ID")
      }

      "DELETE /jobs/<id> should return an error if there is no such id" in {
        val request = sttp.delete(uri"$SJS/jobs/NonExistingId")
        val response = request.send()
        response.code should equal(404)
        val json = Json.parse(response.body.merge)
        (json \ "status").as[String] should equal("ERROR")
        (json \ "result").as[String] should include("No such job ID")
      }
    }

  }

  override def afterAll(configMap: ConfigMap) = {
    // Clean up test entities in general
    sttp.delete(uri"$SJS/binaries/$app")
    sttp.delete(uri"$SJS/binaries/$streamingApp")
    // Clean up test entities just in case something went wrong
    sttp.delete(uri"$SJS/binaries/$appName")
    sttp.delete(uri"$SJS/contexts/$contextName")
    sttp.delete(uri"$SJS/contexts/$batchContextName")
    sttp.delete(uri"$SJS/contexts/$streamingContextName")
  }

}