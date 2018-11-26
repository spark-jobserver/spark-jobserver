package spark.jobserver
import akka.util.ByteString
import spark.jobserver.WebApi._
import spark.jobserver.common.akka.web.JsonUtils.AnyJsonFormat
import spark.jobserver.io.{ContextInfo, ErrorData, JobInfo, JobStatus}

import scala.concurrent.duration.FiniteDuration

object WebApiUtils {
  def errMap(errMsg: String): Map[String, String] =
    Map(StatusKey -> JobStatus.Error, ResultKey -> errMsg)

  def errMap(t: Throwable, status: String): Map[String, Any] =
    Map(StatusKey -> status, ResultKey -> formatException(t))

  def formatException(t: Throwable): Any =
    Map(
      "message" -> t.getMessage,
      "errorClass" -> t.getClass.getName,
      "stack" -> ErrorData.getStackTrace(t)
    )

  def formatException(t: ErrorData): Any = {
    Map(
      "message" -> t.message,
      "errorClass" -> t.errorClass,
      "stack" -> t.stackTrace
    )
  }
  def successMap(msg: String): Map[String, String] =
    Map(StatusKey -> "SUCCESS", ResultKey -> msg)
  def resultToTable(result: Any): Map[String, Any] = {
    Map(ResultKey -> result)
  }
  def resultToMap(result: Any): Map[String, Any] = result match {
    case m: Map[_, _] => m.map { case (k, v) => (k.toString, v) }
    case s: Seq[_] =>
      s.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case a: Array[_] =>
      a.toSeq.zipWithIndex.map { case (item, idx) => (idx.toString, item) }.toMap
    case item => Map(ResultKey -> item)
  }
  def resultToByteIterator(jobReport: Map[String, Any],
                           result: Iterator[_]): Iterator[Byte] = {
    ResultKeyStartBytes.toIterator ++
      (jobReport
        .map(
          t =>
            Seq(
              AnyJsonFormat.write(t._1).toString(),
              AnyJsonFormat.write(t._2).toString()
            ).mkString(":")
        )
        .mkString(",") ++
        (if (jobReport.nonEmpty) "," else "")).getBytes().toIterator ++
      ResultKeyBytes.toIterator ++ result.asInstanceOf[Iterator[Byte]] ++ ResultKeyEndBytes.toIterator
  }
  def getJobDurationString(info: JobInfo): String =
    info.jobLengthMillis
      .map { ms => ms / 1000.0 + " secs"
      }
      .getOrElse("Job not done yet")
  def getJobReport(jobInfo: JobInfo,
                   jobStarted: Boolean = false): Map[String, Any] = {

    val statusMap = jobInfo match {
      case JobInfo(_, _, _, _, _, state, _, _, Some(err)) =>
        Map(StatusKey -> state, ResultKey -> WebApiUtils.formatException(err))
      case JobInfo(_, _, _, _, _, _, _, _, None) if jobStarted =>
        Map(StatusKey -> JobStatus.Started)
      case JobInfo(_, _, _, _, _, state, _, _, None) => Map(StatusKey -> state)
    }
    Map(
      "jobId" -> jobInfo.jobId,
      "startTime" -> jobInfo.startTime.toString(),
      "classPath" -> jobInfo.classPath,
      "context" -> (if (jobInfo.contextName.isEmpty) {"<<ad-hoc>>"}
                    else {jobInfo.contextName}),
      "contextId" -> jobInfo.contextId,
      "duration" -> WebApiUtils.getJobDurationString(jobInfo)
    ) ++ statusMap
  }
  def getContextReport(context: Any,
                       appId: Option[String],
                       url: Option[String]): Map[String, String] = {
    import scala.collection.mutable
    val map = mutable.Map.empty[String, String]
    context match {
      case contextInfo: ContextInfo =>
        map("id") = contextInfo.id
        map("name") = contextInfo.name
        map("startTime") = contextInfo.startTime.toString()
        map("endTime") =
          if (contextInfo.endTime.isDefined){contextInfo.endTime.get.toString}
          else {"Empty"}
        map("state") = contextInfo.state
      case name: String =>
        map("name") = name
      case _ =>
    }
    (appId, url) match {
      case (Some(id), Some(u)) =>
        map("applicationId") = id
        map("url") = u
      case (Some(id), None) =>
        map("applicationId") = id
      case _ =>
    }
    map.toMap
  }
  val dataToByteStr : Byte => ByteString = b => ByteString(b)
  val StatusKey = "status"
  val ResultKey = "result"

  import scala.concurrent.duration._
  val DefaultSyncTimeout: FiniteDuration = 10 seconds
  val DefaultJobLimit = 50
}
