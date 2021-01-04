package spark.jobserver.util

import akka.http.scaladsl.model.Uri
import org.joda.time.DateTime
import spark.jobserver.common.akka.web.JsonUtils.AnyJsonFormat

import java.text.SimpleDateFormat
import spark.jobserver.io.{BinaryInfo, BinaryType, ContextInfo, JobInfo, JobStatus}
import spray.json._

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
        "classPath" -> JsString(i.mainClass),
        "state" -> JsString(i.state),
        "startTime" -> JsString(df.format(i.startTime.getMillis)),
        "endTime" -> i.endTime.map(et => fromJoda(et)).toJson,
        "error" -> i.error.toJson,
        "cp" -> JsArray(i.cp.toVector.map(_.toJson))
      )

    def read(value: JsValue): JobInfo = {
      value.asJsObject.getFields("jobId", "contextId", "contextName", "classPath",
        "state", "startTime", "endTime", "error", "cp", "callbackUrl") match {
          // Correct json format
          case Seq(JsString(jobId), JsString(contextId), JsString(contextName),
          JsString(classPath), JsString(state), JsString(startTime), endTime, error, JsArray(cpJson),
          JsString(callbackUrl)) =>
            val endTimeOpt = readOpt(endTime, et => toJoda(et.convertTo[String]))
            val errorOpt = error.convertTo[Option[ErrorData]]
            val startTimeObj = toJoda(startTime)
            val cp = cpJson.map(_.convertTo[BinaryInfo])
            JobInfo(jobId, contextId, contextName, classPath,
              state, startTimeObj, endTimeOpt, errorOpt, cp,
              Option(callbackUrl).filter(_.trim.nonEmpty).map(Uri(_)))
          // Legacy json format (for backwards compatibility)
          case Seq(JsString(jobId), JsString(contextId), JsString(contextName),
          JsString(classPath), JsString(state), JsString(startTime), endTime, error, JsArray(cpJson)) =>
            val endTimeOpt = readOpt(endTime, et => toJoda(et.convertTo[String]))
            val errorOpt = error.convertTo[Option[ErrorData]]
            val startTimeObj = toJoda(startTime)
            val cp = cpJson.map(_.convertTo[BinaryInfo])
            JobInfo(jobId, contextId, contextName, classPath,
              state, startTimeObj, endTimeOpt, errorOpt, cp, None)
          // Legacy json format (for backwards compatibility)
          case Seq(JsString(jobId), JsString(contextId), JsString(contextName),
          JsString(classPath), JsString(state), JsString(startTime), endTime, error) =>
              val endTimeOpt = readOpt(endTime, et => toJoda(et.convertTo[String]))
              val errorOpt = error.convertTo[Option[ErrorData]]
              val startTimeObj = toJoda(startTime)
              JobInfo(jobId, contextId, contextName, classPath,
                state, startTimeObj, endTimeOpt, errorOpt, Seq.empty, None)
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

object ResultMarshalling {
  val ResultKey = "result"
  val StatusKey = "status"

  def formatException(t: Throwable): Map[String, String] =
    Map("message" -> t.getMessage, "errorClass" -> t.getClass.getName, "stack" -> ErrorData.getStackTrace(t))

  def formatException(t: ErrorData): Map[String, String] = {
    Map("message" -> t.message, "errorClass" -> t.errorClass, "stack" -> t.stackTrace
    )
  }

  def exceptionToMap(jobId: String, t: Throwable): Map[String, Any] = {
    Map[String, String]("jobId" -> jobId) ++
      Map(StatusKey -> JobStatus.Error, ResultKey -> formatException(t))
  }

  def resultToTable(result: Any, jobId: Option[String] = None): Map[String, Any] = {
    jobId.map(id => Map[String, Any]("jobId" -> id)).getOrElse(Map.empty) ++
      Map(ResultKey -> result)
  }

  def resultToByteIterator(jobReport: Map[String, Any], result: Iterator[_]): Iterator[Byte] = {
    val it = "{\n".getBytes.toIterator ++
      (jobReport.map(t => Seq(AnyJsonFormat.write(t._1).toString(),
        AnyJsonFormat.write(t._2).toString()).mkString(":")).mkString(",") ++
        (if (jobReport.nonEmpty) "," else "")).getBytes().toIterator ++
      ("\"" + ResultKey + "\":").getBytes.toIterator ++ result ++ "}".getBytes.toIterator
    it.asInstanceOf[Iterator[Byte]]
  }

  def getJobReport(info: JobInfo, jobStarted: Boolean = false): Map[String, Any] = {
    def getJobDurationString =
      info.jobLengthMillis.map { ms => ms / 1000.0 + " secs" }.getOrElse("Job not done yet")

    val statusMap = info match {
      case JobInfo(_, _, _, _, state, _, _, Some(err), _, _) =>
        Map(StatusKey -> state, ResultKey -> formatException(err))
      case JobInfo(_, _, _, _, _, _, _, None, _, _) if jobStarted => Map(StatusKey -> JobStatus.Started)
      case JobInfo(_, _, _, _, state, _, _, None, _, _) => Map(StatusKey -> state)
    }
    Map("jobId" -> info.jobId,
      "startTime" -> info.startTime.toString(),
      "classPath" -> info.mainClass,
      "context" -> (if (info.contextName.isEmpty) "<<ad-hoc>>" else info.contextName),
      "contextId" -> info.contextId,
      "duration" -> getJobDurationString) ++ statusMap
  }
}