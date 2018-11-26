package spark.jobserver
import java.util.NoSuchElementException

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import spray.json.DefaultJsonProtocol._

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, handleExceptions}
import akka.http.scaladsl.server.{Directive0, ExceptionHandler}
import com.typesafe.config.ConfigException
import spark.jobserver.WebApiUtils.errMap

object JobsRouteUtils {
  private val _exceptionHandler = ExceptionHandler {
    case e: NoSuchElementException =>
      complete(
        StatusCodes.NotFound,
        errMap("context " + e.getMessage + " not found")
      )
    case e: ConfigException =>
      complete(
        StatusCodes.BadRequest,
        errMap("Cannot parse config: " + e.getMessage)
      )
  }

  val exceptionHandler: Directive0 = handleExceptions(_exceptionHandler)
}