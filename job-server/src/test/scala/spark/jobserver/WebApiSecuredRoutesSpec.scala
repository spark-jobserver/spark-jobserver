package spark.jobserver

import com.typesafe.config.ConfigFactory
import spark.jobserver.io.{BinaryType, JobStatus}
import spray.http.HttpHeaders.Authorization
import spray.http.StatusCodes._
import spray.http._

// Tests web response codes and formatting
// Does NOT test underlying Supervisor / JarManager functionality
// HttpService trait is needed for the sealRoute() which wraps exception handling
class WebApiSecuredRoutesSpec extends WebApiSecuredSpec {
  import spark.jobserver.common.akka.web.JsonUtils._
  import spray.json.DefaultJsonProtocol._

  val getJobStatusInfoMap = {
    Map(
      "jobId" -> "foo-1",
      "startTime" -> "2013-05-29T00:00:00.000Z",
      "classPath" -> "com.abc.meme",
      "context"  -> "context",
      "duration" -> "300.0 secs",
      StatusKey -> JobStatus.Finished)
  }

  describe("jars routes") {
    it("should list all jars") {
      Get("/jars").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
          status should be (OK)
          responseAs[Map[String, String]] should be (Map("demo1" -> "2013-05-29T00:00:00.000Z",
                                                     "demo2" -> "2013-05-29T01:00:00.000Z"))
      }
    }

    it("should respond with OK if jar uploaded successfully") {
      Post("/jars/foobar", Array[Byte](0, 1, 2)).
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
          status should be (OK)
      }
    }
  }

  describe("binaries routes") {
    it("should list all binaries") {
      Get("/binaries").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, Map[String, String]]] should be (Map(
          "demo1" -> Map("binary-type" -> "Jar", "upload-time" -> "2013-05-29T00:00:00.000Z"),
          "demo2" -> Map("binary-type" -> "Jar", "upload-time" -> "2013-05-29T01:00:00.000Z"),
          "demo3" -> Map("binary-type" -> "Egg", "upload-time" -> "2013-05-29T02:00:00.000Z")))
      }
    }

    it("should respond with OK if jar uploaded successfully") {
      Post("/binaries/foobar", Array[Byte](0, 1, 2)).
        withHeaders(BinaryType.Jar.contentType, Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
      }
    }

    it("should respond with OK if deleted successfully") {
      Delete("/binaries/foobar").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
      }
    }
  }

  describe("list jobs") {
    it("should list jobs correctly") {
      Get("/jobs").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Seq[Map[String, String]]] should be (Seq(
          Map("jobId" -> "foo-1",
              "startTime" -> "2013-05-29T00:00:00.000Z",
              "classPath" -> "com.abc.meme",
              "context"  -> "context",
              "duration" -> "Job not done yet",
              StatusKey -> JobStatus.Running),
          Map("jobId" -> "foo-1",
              "startTime" -> "2013-05-29T00:00:00.000Z",
              "classPath" -> "com.abc.meme",
              "context"  -> "context",
              "duration" -> "300.0 secs",
              StatusKey -> JobStatus.Finished)
        ))
      }
    }
  }

  describe("/jobs routes") {

    it("async route recognize proxy user") {
      val config2 = "foo.baz = booboo, spark.master=overriden"
      Post("/jobs?appName=foo&classPath=com.abc.meme", config2).
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
      sealRoute(routes) ~> check {
        status should be (Accepted)
        responseAs[Map[String, String]] should be (Map(
          "jobId" -> "foo",
          "startTime" -> "2013-05-29T00:00:00.000Z",
          "classPath" -> "com.abc.meme",
          "context"  -> "context",
          "duration" -> "Job not done yet",
          StatusKey -> JobStatus.Started)
        )
      }
    }

    it("should be able to query a running job from /jobs/<id> route") {
      Get("/jobs/_running").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
          "jobId" -> "foo-1",
          "startTime" -> "2013-05-29T00:00:00.000Z",
          "classPath" -> "com.abc.meme",
          "context"  -> "context",
          "duration" -> "Job not done yet",
          StatusKey -> JobStatus.Running
        ))
      }
    }

    it("should be able to query finished job with result from /jobs/<id> route") {
      Get("/jobs/_finished").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
          "jobId" -> "foo-1",
          "startTime" -> "2013-05-29T00:00:00.000Z",
          "classPath" -> "com.abc.meme",
          "context"  -> "context",
          "duration" -> "300.0 secs",
          StatusKey -> JobStatus.Finished,
          ResultKey -> "_finished!!!"
        ))
      }
    }

    it("should be able to kill job from /jobs/<id> route") {
      Delete("/jobs/job_to_kill").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Map[String, String]] should be (Map(
          StatusKey -> JobStatus.Killed
        ))
      }
    }

    it("should be able to query job config from /jobs/<id>/config route") {
      Get("/jobs/foobar/config").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        ConfigFactory.parseString(responseAs[String]) should be (config)
      }
    }

    it("sync route should return Ok with ERROR in JSON response if job failed") {
      Post("/jobs?appName=err&classPath=com.abc.meme&context=one&sync=true", " ").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
          status should be (OK)
          val result = responseAs[Map[String, Any]]
          result(StatusKey) should equal(JobStatus.Error)
          result.keys should equal (Set(JobId, StatusKey, ResultKey))
          val exceptionMap = result(ResultKey).asInstanceOf[Map[String, Any]]
          exceptionMap should contain key ("cause")
          exceptionMap should contain key ("causingClass")
          exceptionMap("cause") should equal ("foo")
          exceptionMap("causingClass").asInstanceOf[String] should include ("IllegalArgumentException")
      }
    }
  }

  describe("context routes") {
    it("should list all contexts") {
      // responseAs[] uses spray-json to convert JSON results back to types for easier checking
      Get("/contexts").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
        responseAs[Seq[String]] should be (Seq("context1", "context2"))
      }
    }

    it("should return OK if stopping known context") {
      Delete("/contexts/one", "").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
      }
    }

    it("should respond OK if starting a with the same name by another user") {
      Post("/contexts/one").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be (OK)
      }
    }
  }

  describe ("The Secured WebApi") {
    it ("should return valid JSON when a jar is uploaded successfully with valid credentials") {
      Post("/jars/test-app", Array[Byte](0, 1, 2)).
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be(OK)
        responseAs[Map[String, String]] should be (Map(
          WebApi.StatusKey -> "SUCCESS",
          WebApi.ResultKey -> "Jar uploaded"
        ))
      }
    }

    it ("should return valid JSON when creating a context with valid credentials") {
      Post("/contexts/test-ctx", "{}").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be(OK)
        responseAs[Map[String, String]] should be (Map(
          WebApi.StatusKey -> "SUCCESS",
          WebApi.ResultKey -> "Context initialized"
        ))
      }
    }

    it ("Should return valid JSON when stopping a context with valid credentials") {
      Delete("/contexts/test-ctx").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be(OK)
        responseAs[Map[String, String]] should be (Map(
          WebApi.StatusKey -> "SUCCESS",
          WebApi.ResultKey -> "Context stopped"
        ))
      }
    }

    it ("Should return an error when stopping with invalid credentials") {
      Delete("/contexts/timeout-ctx").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password2"))) ~>
        sealRoute(routes) ~> check {
        status should be(Unauthorized)
      }
    }

    it ("Should return valid JSON when resetting a context") {
      Put("/contexts?reset=reboot").
        withHeaders(Authorization(BasicHttpCredentials("user1", "password1"))) ~>
        sealRoute(routes) ~> check {
        status should be(OK)
        responseAs[Map[String, String]] should be (Map(
          WebApi.StatusKey -> "SUCCESS",
          WebApi.ResultKey -> "Context reset"
        ))
      }
    }
  }
}

