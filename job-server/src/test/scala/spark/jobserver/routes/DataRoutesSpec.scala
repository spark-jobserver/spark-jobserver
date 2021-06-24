package spark.jobserver.routes

import java.net.URLEncoder

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import spark.jobserver.io.JobStatus
import spark.jobserver.WebApiSpec

class DataRoutesSpec extends WebApiSpec {
  import spray.json.DefaultJsonProtocol._
  import spark.jobserver.common.akka.web.JsonUtils._

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
        val res = responseAs[Map[String, Any]]
        withoutStack(res) should be(Map(
          StatusKey -> JobStatus.Error, ResultKey -> Map("message" -> "errorfileToRemove",
            "errorClass" -> "java.nio.file.FileAlreadyExistsException")))
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
        status should be(NotFound)
        val res = responseAs[Map[String, Any]]
        withoutStack(res) should be(Map(
          StatusKey -> JobStatus.Error, ResultKey -> Map("message" -> "errorfileToRemove",
            "errorClass" -> "java.nio.file.NoSuchFileException")))
      }
    }

    def withoutStack(map: Map[String, Any]): Map[String, Any] = {
      if (map.contains(ResultKey)) {
        val result = map(ResultKey).asInstanceOf[Map[String, String]]
          .filterKeys(k => k != "stack")
        map.filterKeys(k => k != ResultKey) ++ Map(ResultKey -> result)
      }
      else {
        map
      }
    }
  }
}
