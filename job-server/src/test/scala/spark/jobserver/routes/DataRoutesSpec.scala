package spark.jobserver.routes

import java.net.URLEncoder
<<<<<<< 2229a2b141eaed023f1724478313be8dd120f950:job-server/test/spark.jobserver/routes/DataRoutesSpec.scala
import spark.jobserver.io.JobStatus
=======

>>>>>>> Clean up commits for Java API:job-server/src/test/scala/spark/jobserver/routes/DataRoutesSpec.scala
import spark.jobserver.WebApiSpec
import spark.jobserver.common.akka.web.JsonUtils
import spray.http.StatusCodes._

class DataRoutesSpec extends WebApiSpec {
  import JsonUtils._
  import spray.httpx.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  describe("/data routes") {

    it("POST - should be able to post file to tmp dir") {
      val encodedName = URLEncoder.encode("/tmp/fileToRemove", "UTF-8")
      Post("/data/" + encodedName, Array[Byte](0, 1, 2)) ~> sealRoute(routes) ~> check {
        status should be(OK)
        responseAs[Map[String, Any]] should be (Map(
          ResultKey -> Map("filename" -> "/tmp/fileToRemove-time-stamp")
        ))
      }
    }

    it("POST - should report error when receiver reports error") {
      Post("/data/errorfileToRemove", Array[Byte](0, 1, 2)) ~> sealRoute(routes) ~> check {
        status should be(BadRequest)
        responseAs[Map[String, String]] should be(Map(
          StatusKey -> JobStatus.Error, ResultKey -> "Failed to store data file 'errorfileToRemove'."))
      }
    }

    it("GET - should be able to list stored files") {
      Get("/data/") ~> sealRoute(routes) ~> check {
        status should be(OK)
        responseAs[Seq[String]] should be (Seq("demo1", "demo2"))
      }
    }

    it("DELETE - should be able to remove file from tmp dir") {
      val encodedName = URLEncoder.encode("/tmp/fileToRemove", "UTF-8")
      Delete("/data/" + encodedName) ~> sealRoute(routes) ~> check {
        status should be(OK)
      }
    }

    it("DELETE - should report error when receiver reports error") {
      Delete("/data/errorfileToRemove") ~> sealRoute(routes) ~> check {
        status should be(BadRequest)
        responseAs[Map[String, String]] should be(Map(
          StatusKey -> JobStatus.Error, ResultKey -> "Unable to delete data file 'errorfileToRemove'."))
      }
    }

  }
}
