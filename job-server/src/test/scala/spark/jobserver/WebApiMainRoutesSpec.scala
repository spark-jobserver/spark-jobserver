package spark.jobserver

import com.typesafe.config.ConfigFactory
import spark.jobserver.io.BinaryType
import spray.http.{HttpHeader, HttpHeaders, MediaTypes}
import spray.http.StatusCodes._
import spark.jobserver.io.{ContextStatus, JobStatus}
import spark.jobserver.util.SparkJobUtils

// Tests web response codes and formatting
// Does NOT test underlying Supervisor / JarManager functionality
// HttpService trait is needed for the sealRoute() which wraps exception handling
class WebApiMainRoutesSpec extends WebApiSpec {
  import spark.jobserver.common.akka.web.JsonUtils._
  import spray.httpx.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  val getJobStatusInfoMap = {
    Map(
      "jobId" -> "foo-1",
      "contextId" -> "cid",
      "startTime" -> "2013-05-29T00:00:00.000Z",
      "classPath" -> "com.abc.meme",
      "context"  -> "context",
      "duration" -> "300.0 secs",
      StatusKey -> JobStatus.Finished)
  }

  describe("binaries routes") {
    it("should list all binaries") {
      Get("/binaries") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Map[String, String]]] should be (Map(
          "demo1" -> Map("binary-type" -> "Jar", "upload-time" -> "2013-05-29T00:00:00.000Z"),
          "demo2" -> Map("binary-type" -> "Jar", "upload-time" -> "2013-05-29T01:00:00.000Z"),
          "demo3" -> Map("binary-type" -> "Egg", "upload-time" -> "2013-05-29T02:00:00.000Z")))
      }
    }

    it("should respond with OK if jar uploaded successfully") {
      Post("/binaries/foobar", Array[Byte](0, 1, 2)).
        withHeaders(BinaryType.Jar.contentType) ~> sealRoute(routes) ~> check {
        status should be (OK)
      }
    }

    it("should respond with OK if egg uploaded successfully") {
      Post("/binaries/pyfoo", Array[Byte](0, 1, 2)).
        withHeaders(BinaryType.Egg.contentType) ~> sealRoute(routes) ~> check {
        status should be (OK)
      }
    }

    it("should respond with Unsupported Media Type if upload attempted without content type header") {
      Post("/binaries/foobar", Array[Byte](0, 1, 2)) ~> sealRoute(routes) ~> check {
        status should be (UnsupportedMediaType)
      }
    }

    it("should respond with Unsupported Media Type if upload attempted with invalid content type header") {
      Post("/binaries/foobar", Array[Byte](0, 1, 2)).
        withHeaders(HttpHeaders.`Content-Type`(MediaTypes.`application/json`)) ~> sealRoute(routes) ~> check {
        status should be (UnsupportedMediaType)
      }
    }

    it("should respond with bad request if jar formatted incorrectly") {
      Post("/binaries/badjar", Array[Byte](0, 1, 2)).
        withHeaders(BinaryType.Jar.contentType) ~> sealRoute(routes) ~> check {
        status should be (BadRequest)
      }
    }

    it("should respond with internal server error if storage fails") {
      Post("/binaries/daofail", Array[Byte](0, 1, 2)).
        withHeaders(BinaryType.Jar.contentType) ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
      }
    }

    it("should respond with OK if deleted successfully") {
      Delete("/binaries/foobar") ~> sealRoute(routes) ~> check {
        status should be (OK)
      }
    }

    it("should respond with 404 Not Found if binary was not found during deletion") {
      Delete("/binaries/badbinary") ~> sealRoute(routes) ~> check {
        status should be (NotFound)
      }
    }

    it("should respond with 403 forbidden if user is deleting an active jar") {
      Delete("/binaries/active") ~> sealRoute(routes) ~> check {
        status should be (Forbidden)
        responseAs[Map[String, String]] should be (
          Map("status" -> "ERROR",
            "result" -> "Binary is in use by job(s): job-active")
        )
      }
    }

    it("should handle failures of dao layer gracefully") {
      Delete("/binaries/failure") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        responseAs[Map[String, String]] should be (
          Map("status" -> "ERROR",
            "result" -> "Failed to delete binary due to internal error. Check logs.")
        )
      }
    }
  }

  describe("list jobs") {
    it("should list jobs correctly") {
      Get("/jobs") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Seq[Map[String, String]]] should be (Seq(
          Map("jobId" -> "foo-1",
              "contextId" -> "cid",
              "startTime" -> "2013-05-29T00:00:00.000Z",
              "classPath" -> "com.abc.meme",
              "context"  -> "context",
              "duration" -> "Job not done yet",
              StatusKey -> JobStatus.Running),
          Map("jobId" -> "foo-1",
              "contextId" -> "cid",
              "startTime" -> "2013-05-29T00:00:00.000Z",
              "classPath" -> "com.abc.meme",
              "context"  -> "context",
              "duration" -> "300.0 secs",
              StatusKey -> JobStatus.Finished)
        ))
      }
    }
    it("should list finished jobs") {
      Get("/jobs?status=finished") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Seq[Map[String, String]]] should be (Seq(
          Map("jobId" -> "foo-1",
            "contextId" -> "cid",
            "startTime" -> "2013-05-29T00:00:00.000Z",
            "classPath" -> "com.abc.meme",
            "context"  -> "context",
            "duration" -> "300.0 secs",
            StatusKey -> JobStatus.Finished)
        ))
      }
    }
    it("should list error jobs") {
      Get("/jobs?status=error") ~> sealRoute(routes) ~> check {
        status should be (OK)
        val result = responseAs[Seq[Map[String, Any]]].head
        result(StatusKey) should equal(JobStatus.Error)

        val exceptionMap = result(ResultKey).asInstanceOf[Map[String, Any]]
        exceptionMap("message") should equal ("test-error")
      }
    }
    it("should list killed jobs") {
      Get("/jobs?status=killed") ~> sealRoute(routes) ~> check {
        status should be (OK)
        val result = responseAs[Seq[Map[String, Any]]].head
        result(StatusKey) should equal(JobStatus.Killed)

        val exceptionMap = result(ResultKey).asInstanceOf[Map[String, Any]]
        exceptionMap("message") should equal ("Job foo-1 killed")
      }
    }
    it("should list running jobs") {
      Get("/jobs?status=running") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Seq[Map[String, String]]] should be (Seq(
          Map("jobId" -> "foo-1",
            "contextId" -> "cid",
            "startTime" -> "2013-05-29T00:00:00.000Z",
            "classPath" -> "com.abc.meme",
            "context"  -> "context",
            "duration" -> "Job not done yet",
            StatusKey -> JobStatus.Running)
        ))
      }
    }
  }

  describe("/jobs routes") {
    it("should respond with bad request if jobConfig cannot be parsed") {
      Post("/jobs?appName=foo&classPath=com.abc.meme", "Not parseable jobConfig!!") ~>
          sealRoute(routes) ~> check {
        status should be (BadRequest)
        val result = responseAs[Map[String, String]]
        result(StatusKey) should equal(JobStatus.Error)
        result(ResultKey) should startWith ("Cannot parse")
      }
      Post("/jobs?appName=foo&classPath=com.abc.meme&sync=true", "Not parseable jobConfig!!") ~>
          sealRoute(routes) ~> check {
        status should be (BadRequest)
        val result = responseAs[Map[String, String]]
        result(StatusKey) should equal(JobStatus.Error)
        result(ResultKey) should startWith ("Cannot parse")
      }
    }

    it("should merge user passed jobConfig with default jobConfig") {
      val config2 = "foo.baz = booboo, spark.master=overriden"
      Post("/jobs?appName=foo&classPath=com.abc.meme&context=one&sync=true", config2) ~>
          sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (Map(
          JobId -> "foo",
          ResultKey -> Map(
            masterConfKey->"overriden",
            bindConfKey -> bindConfVal,
            "foo.baz" -> "booboo",
            "shiro.authentication" -> "off",
            "spark.jobserver.short-timeout" -> "3 s"
          )
        ))
      }
    }

    it("async route should return 202 if job starts successfully") {
      Post("/jobs?appName=foo&classPath=com.abc.meme&context=one", "") ~> sealRoute(routes) ~> check {
        status should be (Accepted)
        responseAs[Map[String, String]] should be (Map(
          "jobId" -> "foo",
          "contextId" -> "cid",
          "startTime" -> "2013-05-29T00:00:00.000Z",
          "classPath" -> "com.abc.meme",
          "context"  -> "context",
          "duration" -> "Job not done yet",
          StatusKey -> JobStatus.Started)
        )
      }
    }

    it("async route should return 409 if job starts was triggered while the context was in stopping state") {
      Post("/jobs?appName=context-already-stopped&classPath=com.abc.meme&context=one",
          "") ~> sealRoute(routes) ~> check {
        status should be (Conflict)
        val result = responseAs[Map[String, String]]
        result(StatusKey) should equal(JobStatus.Error)
        result(ResultKey) should be("Context stop in progress")
      }
    }

    it("adhoc job of sync route should return 200 and result") {
      val config2 = "foo.baz = booboo"
      Post("/jobs?appName=foo&classPath=com.abc.meme&sync=true", config2) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (Map(
          JobId -> "foo",
          ResultKey -> Map(
            masterConfKey -> masterConfVal,
            bindConfKey -> bindConfVal,
            "foo.baz" -> "booboo",
            "shiro.authentication" -> "off",
            "spark.jobserver.short-timeout" -> "3 s"
          )
        ))
      }
    }

    it("adhoc job with Stream result of sync route should return 200 and chunked result") {
      val config2 = "foo.baz = booboo"
      Post("/jobs?appName=foo.stream&classPath=com.abc.meme&sync=true", config2) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (Map(
          ResultKey -> "1, 2, 3, 4, 5, 6"
        ))
      }
    }

    it("should be able to take a timeout param") {
      val config2 = "foo.baz = booboo"
      Post("/jobs?appName=foo&classPath=com.abc.meme&sync=true&timeout=5", config2) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (Map(
          JobId -> "foo",
          ResultKey -> Map(
            masterConfKey -> masterConfVal,
            bindConfKey -> bindConfVal,
            "foo.baz" -> "booboo",
            "shiro.authentication" -> "off",
            "spark.jobserver.short-timeout" -> "3 s"
          )
        ))
      }
    }

    it("adhoc job started successfully of async route should return 202") {
      Post("/jobs?appName=foo&classPath=com.abc.meme", "") ~> sealRoute(routes) ~> check {
        status should be (Accepted)
        responseAs[Map[String, String]] should be (Map(
          "jobId" -> "foo",
          "contextId" -> "cid",
          "startTime" -> "2013-05-29T00:00:00.000Z",
          "classPath" -> "com.abc.meme",
          "context"  -> "context",
          "duration" -> "Job not done yet",
          StatusKey -> JobStatus.Started)
        )
      }
    }

    it("should be able to query a running job from /jobs/<id> route") {
      Get("/jobs/_running") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
          "jobId" -> "foo-1",
          "contextId" -> "cid",
          "startTime" -> "2013-05-29T00:00:00.000Z",
          "classPath" -> "com.abc.meme",
          "context"  -> "context",
          "duration" -> "Job not done yet",
          StatusKey -> JobStatus.Running
        ))
      }
    }

    it("should be able to query finished job with result from /jobs/<id> route") {
      Get("/jobs/_finished") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
          "jobId" -> "foo-1",
          "contextId" -> "cid",
          "startTime" -> "2013-05-29T00:00:00.000Z",
          "classPath" -> "com.abc.meme",
          "context"  -> "context",
          "duration" -> "300.0 secs",
          StatusKey -> JobStatus.Finished,
          ResultKey -> "_finished!!!"
        ))
      }
    }

    it("should respond with 404 Not Found and meaningful message if status of jobId does not exist") {
      Get("/jobs/_no_status") ~> sealRoute(routes) ~> check {
        status should be (NotFound)
        val result = responseAs[Map[String, String]]
        result(ResultKey) should startWith ("No such job ID ")
      }
    }

    it("should be able to kill job from /jobs/<id> route") {
      Delete("/jobs/job_to_kill") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
          StatusKey -> JobStatus.Killed
        ))
      }
    }

    it("should be able to query job config from /jobs/<id>/config route") {
      Get("/jobs/foobar/config") ~> sealRoute(routes) ~> check {
        status should be (OK)
        ConfigFactory.parseString(responseAs[String]) should be (config)
      }
    }

    it("should respond with 404 Not Found from /jobs/<id>/config route if jobId does not exist") {
      Get("/jobs/badjobid/config") ~> sealRoute(routes) ~> check {
        status should be (NotFound)
      }
    }

    it("should respond with 404 Not Found if context does not exist") {
      Post("/jobs?appName=foo&classPath=com.abc.meme&context=no-context", " ") ~> sealRoute(routes) ~> check {
        status should be (NotFound)
        val resultMap = responseAs[Map[String, String]]
        resultMap(StatusKey) should be (JobStatus.Error)
      }
    }

    it("should respond with 404 Not Found if app or class not found") {
      Post("/jobs?appName=no-app&classPath=com.abc.meme&context=one", " ") ~> sealRoute(routes) ~> check {
        status should be (NotFound)
        val resultMap = responseAs[Map[String, String]]
        resultMap(StatusKey) should be (JobStatus.Error)
      }

      Post("/jobs?appName=foobar&classPath=no-class&context=one", " ") ~> sealRoute(routes) ~> check {
        status should be (NotFound)
      }
    }

    it("should respond with 400 if job is of the wrong type") {
      Post("/jobs?appName=wrong-type&classPath=com.abc.meme", " ") ~> sealRoute(routes) ~> check {
        status should be (BadRequest)
        val resultMap = responseAs[Map[String, String]]
        resultMap(StatusKey) should be (JobStatus.Error)
      }
    }

    it("sync route should return Ok with ERROR in JSON response if job failed") {
      Post("/jobs?appName=err&classPath=com.abc.meme&context=one&sync=true", " ") ~>
          sealRoute(routes) ~> check {
        status should be (OK)
        val result = responseAs[Map[String, Any]]
        result(StatusKey) should equal(JobStatus.Error)
        result.keys should equal (Set(JobId, StatusKey, ResultKey))
        val exceptionMap = result(ResultKey).asInstanceOf[Map[String, Any]]
        exceptionMap should contain key ("stack")
        exceptionMap("stack").asInstanceOf[String] should include ("foo")
        exceptionMap("stack").asInstanceOf[String] should include ("IllegalArgumentException")
      }
    }
  }

  describe("serializing complex data types") {
    it("should be able to serialize nested Seq's and Map's within Map's to JSON") {
      Get("/jobs/_mapseq") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (
          getJobStatusInfoMap ++ Map(ResultKey -> Map("first" -> Seq(1, 2, Seq("a", "b")))))
      }

      Get("/jobs/_mapmap") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (
          getJobStatusInfoMap ++ Map(ResultKey -> Map("second" -> Map("K" -> Map("one" -> 1)))
        ))
      }
    }

    it("should be able to serialize Seq's with different types to JSON") {
      Get("/jobs/_seq") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (
          getJobStatusInfoMap ++ Map(ResultKey -> Seq(1, 2, Map("3" -> "three"))
          )
        )
      }
    }

    it("should be able to chunk serialize Stream with different types to JSON") {
      Get("/jobs/_stream") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (
          getJobStatusInfoMap ++ Map(ResultKey -> "1, 2, 3, 4, 5, 6, 7")
        )
      }
    }

    it("should be able to serialize base types (eg float, numbers) to JSON") {
      Get("/jobs/_num") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (
          getJobStatusInfoMap ++ Map(ResultKey -> 5000)
        )
      }
    }

    it("should convert non-understood types to string") {
      Get("/jobs/_unk") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Any]] should be (
          getJobStatusInfoMap ++ Map(ResultKey -> Seq(1,  "101"))
        )
      }
    }
  }

  describe("context routes") {
    it("should list all contexts") {
      // responseAs[] uses spray-json to convert JSON results back to types for easier checking
      Get("/contexts") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Seq[String]] should be (Seq("context1", "context2"))
      }
    }

    it("should return context information with context UI url (local mode)") {
      Get("/contexts/context1") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
            "name" -> "context1",
            "applicationId" -> "local-1337",
            "url" -> "http://spark:4040"))
      }
    }

    it("should return context information without context UI url (local mode)") {
      Get("/contexts/context2") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
            "name" -> "context2",
            "applicationId" -> "local-1337"))
      }
    }

    it("should return valid contextInfo for running context (cluster/client mode)") {
      Get("/contexts/contextWithInfo") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map("name" -> "contextWithInfo",
                                                       "applicationId" -> "local-1337",
                                                       "url" -> "http://spark:4040",
                                                       "state" -> ContextStatus.Running,
                                                       "id" -> "contextId",
                                                       "endTime" -> "Empty",
                                                       "startTime" -> "2013-05-29T00:00:00.000Z"))
      }
    }

    it("should return valid contextInfo for finished context (cluster/client mode)") {
      Get("/contexts/finishedContextWithInfo") ~> sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map("name" -> "finishedContextWithInfo",
                                                       "state" -> ContextStatus.Finished,
                                                       "id" -> "contextId",
                                                       "endTime" -> "2013-05-29T00:05:00.000Z",
                                                       "startTime" -> "2013-05-29T00:00:00.000Z"))
      }
    }

    it("should respond with InternalServerError if unexpected error occurs at getting context") {
      Get("/contexts/unexp-err", "") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val result = responseAs[Map[String, Any]]
        result(ResultKey) should equal("UNEXPECTED ERROR OCCURRED")
      }
    }

    it("should allow anonymous user to delete context with any impersonation when security is off") {
      Delete("/contexts/xxx?" + SparkJobUtils.SPARK_PROXY_USER_PARAM + "=YYY") ~>
      sealRoute(routes) ~> check {
        status should be(OK)
      }
    }

    it("should return OK if stopping known context") {
      Delete("/contexts/one", "") ~> sealRoute(routes) ~> check {
        status should be (OK)
        val result = responseAs[Map[String, String]]
        result(ResultKey) should equal("Context stopped")
      }
    }

    it("should respond with InternalServerError if timeout occurs on a delete request") {
      Delete("/contexts/timeout-ctx", "") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val result = responseAs[Map[String, Any]]
        result(StatusKey) should equal("CONTEXT DELETE ERROR")
      }
    }

    it("should respond with 404 Not Found if stopping unknown context") {
      Delete("/contexts/none", "") ~> sealRoute(routes) ~> check {
        status should be (NotFound)
        val result = responseAs[Map[String, String]]
        result(ResultKey) should equal("context none not found")
      }
    }

    it("should respond with InternalServerError if unexpected error occurs at deleting context") {
      Delete("/contexts/unexp-err", "") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val result = responseAs[Map[String, Any]]
        result(ResultKey) should equal("UNEXPECTED ERROR OCCURRED")
      }
    }

    it("should respond with 202 and location header if failed to stop context in time") {
      Delete("/contexts/ctx-stop-in-progress", "") ~> sealRoute(routes) ~> check {
        status should be (Accepted)
        header("Location").get.value should be("http://example.com/contexts/ctx-stop-in-progress")
        response.entity.asString should be("")
      }
    }

    it("should respond with bad request if starting an already started context") {
      Post("/contexts/one") ~> sealRoute(routes) ~> check {
        status should be (BadRequest)
        val result = responseAs[Map[String, String]]
        result(StatusKey) should equal(JobStatus.Error)
        result(ResultKey) should equal("context one exists")
      }
    }

    it("should return OK if starting a new context") {
      Post("/contexts/meme?num-cpu-cores=3") ~> sealRoute(routes) ~> check {
        status should be (OK)
        val result = responseAs[Map[String, String]]
        result(ResultKey) should equal("Context initialized")
      }
      Post("/contexts/meme?num-cpu-cores=3&coarse-mesos-mode=true") ~> sealRoute(routes) ~> check {
        status should be (OK)
        val result = responseAs[Map[String, String]]
        result(ResultKey) should equal("Context initialized")
      }
    }

    it("should setup a new context with the correct configurations.") {
      val config =
        """spark.context-settings {
          |  test = 1
          |  override_me = 3
          |}
        """.stripMargin
      Post("/contexts/custom-ctx?num-cpu-cores=2&override_me=2", config) ~> sealRoute(routes) ~> check {
        status should be (OK)
      }
    }

    it("should respond with InternalServerError if initialization error occurs") {
      Post("/contexts/initError-ctx", "") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val result = responseAs[Map[String, Any]]
        result(StatusKey) should equal("CONTEXT INIT ERROR")
      }
    }

    it("should respond with InternalServerError if unexpected error occurs at adding context") {
      Post("/contexts/unexp-err", "") ~> sealRoute(routes) ~> check {
        status should be (InternalServerError)
        val result = responseAs[Map[String, Any]]
        result(ResultKey) should equal("UNEXPECTED ERROR OCCURRED")
      }
    }
  }
}

