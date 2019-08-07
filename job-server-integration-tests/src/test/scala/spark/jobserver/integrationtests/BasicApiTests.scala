package spark.jobserver.integrationtests

import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import com.softwaremill.sttp._

import play.api.libs.json.JsObject
import play.api.libs.json.Json

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

  "/binaries" - {

    val appName = "IntegrationTestApp"
    var binaryUploadDate : DateTime = null

    "POST /binaries/<app> should upload a binary" in {
      val byteArray = fileToByteArray(bin)

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
      val byteArray = fileToByteArray(streamingbin)
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
    val contextName = "IntegrationTestContext"

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
      // status finished?
      val request2 = sttp.get(uri"$SJS/contexts/$contextName")
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

    val app = "job-server-tests"
    val streamingApp = "job-server-streaming-test"
    var adHocJobId : String = ""
    var batchJobId: String = ""
    var streamingJobId : String = ""
    var jobContext : String = ""

    "(preparation) uploading job binaries for job testing should not fail" in {
      val byteArray1 = fileToByteArray(bin)
      val byteArray2 = fileToByteArray(streamingbin)
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
        val request = sttp.get(uri"$SJS/contexts/$jobContext")
        var response = request.send()
        response.code should equal(200)
        var json = Json.parse(response.body.merge)
        var state = (json \ "state").as[String]
        // Stopping may take some time, so retry
        var retries = 5
        while(state != "FINISHED" && retries > 0){
          Thread.sleep(1000)
          response = request.send()
          response.code should equal(200)
          json = Json.parse(response.body.merge)
          state = (json \ "state").as[String]
          retries -= 1
        }
        state should equal("FINISHED")
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
        jobContext = "TestBatchContext"
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
        jobContext = "TestStreamingContext"
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

  /*
   * Helper methods
   */

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

}