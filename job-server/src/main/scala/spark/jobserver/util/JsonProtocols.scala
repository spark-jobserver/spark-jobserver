package spark.jobserver.util

import org.joda.time.DateTime
import java.text.SimpleDateFormat
import spark.jobserver.io.{BinaryInfo, BinaryType, ContextInfo, ErrorData, JobInfo}
import spray.json.{DefaultJsonProtocol, JsNull, JsObject, JsString, JsValue, RootJsonFormat,
                   deserializationError, pimpAny}

object JsonProtocols extends DefaultJsonProtocol {

  val DATE_PATTERN = "yyyy-MM-dd HH-mm-ss SS Z"

  /*
   * BinaryInfo
   */

  implicit object BinaryInfoJsonFormat extends RootJsonFormat[BinaryInfo] {
    def write(c: BinaryInfo): JsObject =
      JsObject(
        "appName" -> JsString(c.appName),
        "binaryType" -> JsString(c.binaryType.name),
        "uploadTime" -> JsString(fromJoda(c.uploadTime)),
        "binaryStorageId" -> c.binaryStorageId.toJson)

    def read(value: JsValue): BinaryInfo = {
      value.asJsObject.getFields("appName", "binaryType", "uploadTime", "binaryStorageId") match {
        // Correct json format
        case Seq(JsString(appName), JsString(binaryType), JsString(uploadTime), binaryStorageId) =>
          val binStorageIdOpt = readOpt(binaryStorageId, b => b.convertTo[String])
          val uploadTimeObj = toJoda(uploadTime)
          BinaryInfo(appName, BinaryType.fromString(binaryType), uploadTimeObj, binStorageIdOpt)
        // Incorrect json format
        case _ => deserializationError("Fail to parse json content:" +
          "The following Json structure does not match BinaryInfo structure:\n" + value.prettyPrint)
      }
    }
  }

  /*
   * JobInfo
   */

  implicit object JobInfoJsonFormat extends RootJsonFormat[JobInfo] {
    implicit val errorDataFormat = jsonFormat3(ErrorData.apply)
    def write(i: JobInfo): JsObject =
      JsObject(
        "jobId" -> JsString(i.jobId),
        "contextId" -> JsString(i.contextId),
        "contextName" -> JsString(i.contextName),
        "binaryInfo" -> i.binaryInfo.toJson,
        "classPath" -> JsString(i.classPath),
        "state" -> JsString(i.state),
        "startTime" -> JsString(df.format(i.startTime.getMillis)),
        "endTime" -> i.endTime.map(et => fromJoda(et)).toJson,
        "error" -> i.error.toJson)

    def read(value: JsValue): JobInfo = {
      value.asJsObject.getFields("jobId", "contextId", "contextName", "binaryInfo", "classPath",
        "state", "startTime", "endTime", "error") match {
          // Correct json format
          case Seq(JsString(jobId), JsString(contextId), JsString(contextName),
            binaryInfo, JsString(classPath), JsString(state), JsString(startTime), endTime, error) =>
            val endTimeOpt = readOpt(endTime, et => toJoda(et.convertTo[String]))
            val errorOpt = error.convertTo[Option[ErrorData]]
            val startTimeObj = toJoda(startTime)
            JobInfo(jobId, contextId, contextName, binaryInfo.convertTo[BinaryInfo], classPath,
              state, startTimeObj, endTimeOpt, errorOpt)
          // Incorrect json format
          case _ => deserializationError("Fail to parse json content:" +
            "The following Json structure does not match JobInfo structure:\n" + value.prettyPrint)
        }
    }

  }

  /*
   * ContextInfo
   */

  implicit object ContextInfoJsonFormat extends RootJsonFormat[ContextInfo] {
    def write(i: ContextInfo): JsObject =
      JsObject(
        "id" -> JsString(i.id),
        "name" -> JsString(i.name),
        "config" -> JsString(i.config),
        "actorAddress" -> i.actorAddress.toJson,
        "startTime" -> JsString(fromJoda(i.startTime)),
        "endTime" -> i.endTime.map(et => fromJoda(et)).toJson,
        "state" -> JsString(i.state),
        "error" -> i.error.map(e => e.getMessage).toJson)

    def read(value: JsValue): ContextInfo = {
      value.asJsObject.getFields("id", "name", "config", "actorAddress", "startTime",
        "endTime", "state", "error") match {
          // Correct json format
          case Seq(JsString(id), JsString(name), JsString(config),
            actorAddress, JsString(startTime), endTime, JsString(state), error) =>
            val actorAddressOpt = readOpt(actorAddress, a => a.convertTo[String])
            val endTimeOpt = readOpt(endTime, et => toJoda(et.convertTo[String]))
            val errorOpt = readOpt(error, e => new Throwable(e.convertTo[String]))
            val startTimeObj = toJoda(startTime)
            ContextInfo(id, name, config, actorAddressOpt, startTimeObj, endTimeOpt, state, errorOpt)
          // Incorrect json format
          case _ => deserializationError("Fail to parse json content:" +
            "The following Json structure does not match ContextInfo structure:\n" + value.prettyPrint)
        }
    }

  }

  /*
   * Helpers
   */

  private def df : SimpleDateFormat = new SimpleDateFormat(DATE_PATTERN)
  private def fromJoda(dt: DateTime): String = df.format(dt.getMillis)
  private def toJoda(s: String): DateTime = new DateTime(df.parse(s).getTime)
  private def readOpt[A](j: JsValue, f: JsValue => A): Option[A] = {
    if (j == JsNull) None else Some(f(j))
  }

}
