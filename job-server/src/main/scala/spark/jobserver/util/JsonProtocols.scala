package spark.jobserver.util

import org.joda.time.DateTime
import java.text.SimpleDateFormat
import spark.jobserver.io.{BinaryInfo, BinaryType}
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, RootJsonFormat, deserializationError}

object JsonProtocols extends DefaultJsonProtocol {
  val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss SS Z")

  implicit object BinaryInfoJson extends RootJsonFormat[BinaryInfo] {
    def write(c: BinaryInfo): JsObject =
      JsObject(
        "appName" -> JsString(c.appName),
        "binaryType" -> JsString(c.binaryType.name),
        "uploadTime" -> JsString(df.format(c.uploadTime.getMillis)),
        "binaryStorageId" -> JsString(c.binaryStorageId.getOrElse("null"))
      )

    def read(value: JsValue): BinaryInfo = {
      value.asJsObject.getFields("appName", "binaryType", "uploadTime", "binaryStorageId") match {
        case Seq(JsString(appName), JsString(binaryType), JsString(uploadTime), JsString(binaryStorageId)) =>
          val binId = if (binaryStorageId == "null") None else Some(binaryStorageId)
          BinaryInfo(appName, BinaryType.fromString(binaryType),
            new DateTime(df.parse(uploadTime).getTime), binId)
        case _ => deserializationError("Fail to parse json content: BinaryInfo expected")
      }
    }
  }

}
