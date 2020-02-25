package spark.jobserver.util

import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.Matchers

import spark.jobserver.io.BinaryInfo
import spark.jobserver.io.BinaryType
import spark.jobserver.io.JobInfo
import spark.jobserver.util.JsonProtocols._
import spray.json.pimpAny
import spray.json.pimpString
import spark.jobserver.io.ContextInfo
import java.text.SimpleDateFormat

class JsonProtocolsSpec extends FunSpec with Matchers with BeforeAndAfter {

  /*
   * Test data
   */

  // Date formatting
  val df = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss SS Z")
  val earlyDate = new DateTime().minusHours(1)
  val date = new DateTime()
  val earlyDateStr = df.format(earlyDate.getMillis)
  val dateStr = df.format(date.getMillis)

  // BinaryInfo
  val testBinaryInfo = BinaryInfo("SomeName", BinaryType.Jar, earlyDate, Some("SomeStorId"))
  val testBinaryInfoJson = "{\"appName\":\"SomeName\",\"binaryType\":\"Jar\",\"uploadTime\":\"" + earlyDateStr + "\",\"binaryStorageId\":\"SomeStorId\"}"
  val testBinaryInfo2 = BinaryInfo("SomeName", BinaryType.Egg, earlyDate, None)
  val testBinaryInfo2Json = "{\"appName\":\"SomeName\",\"binaryType\":\"Egg\",\"uploadTime\":\"" + earlyDateStr + "\",\"binaryStorageId\":null}"
  val testBinaryInfo3 = BinaryInfo("file://some/path/to/file", BinaryType.URI, earlyDate, None)
  val testBinaryInfo3Json = "{\"appName\":\"file://some/path/to/file\",\"binaryType\":\"Uri\",\"uploadTime\":\"" + earlyDateStr + "\",\"binaryStorageId\":null}"

  // JobInfo
  val testJobInfo = JobInfo("SomeJobId", "SomeContextId", "SomeContextName",
      "SomeClassPath", "SomeState", earlyDate, Some(date), Some(ErrorData("SomeMessage",
      "SomeClass", "SomeTrace")), Seq.empty)
  val testJobInfo2 = JobInfo("SomeJobId", "SomeContextId", "SomeContextName",
      "SomeClassPath", "SomeState", earlyDate, None, None, Seq.empty)
  val testJobInfoWithCp = JobInfo("SomeJobId", "SomeContextId", "SomeContextName",
    "SomeClassPath", "SomeState", earlyDate, Some(date), Some(ErrorData("SomeMessage",
      "SomeClass", "SomeTrace")), Seq(testBinaryInfo, testBinaryInfo2))
  val testJobInfoJson = "{\"classPath\":\"SomeClassPath\",\"startTime\":\"" + earlyDateStr + "\",\"state\":\"SomeState\",\"contextName\":\"SomeContextName\",\"endTime\":\"" + dateStr + "\",\"error\":{\"message\":\"SomeMessage\",\"errorClass\":\"SomeClass\",\"stackTrace\":\"SomeTrace\"},\"jobId\":\"SomeJobId\",\"cp\":[],\"contextId\":\"SomeContextId\"}"
  val testJobInfoNoCpJson = "{\"classPath\":\"SomeClassPath\",\"startTime\":\"" + earlyDateStr + "\",\"state\":\"SomeState\",\"contextName\":\"SomeContextName\",\"endTime\":\"" + dateStr + "\",\"error\":{\"message\":\"SomeMessage\",\"errorClass\":\"SomeClass\",\"stackTrace\":\"SomeTrace\"},\"jobId\":\"SomeJobId\",\"contextId\":\"SomeContextId\"}"
  val testJobInfo2Json = "{\"classPath\":\"SomeClassPath\",\"startTime\":\"" + earlyDateStr + "\",\"state\":\"SomeState\",\"contextName\":\"SomeContextName\",\"endTime\":null,\"error\":null,\"jobId\":\"SomeJobId\",\"cp\":[],\"contextId\":\"SomeContextId\"}"
  val testJobInfoWithNonEmptyCpJson = "{\"classPath\":\"SomeClassPath\",\"startTime\":\"" + earlyDateStr + "\",\"state\":\"SomeState\",\"contextName\":\"SomeContextName\",\"endTime\":\"" + dateStr + "\",\"error\":{\"message\":\"SomeMessage\",\"errorClass\":\"SomeClass\",\"stackTrace\":\"SomeTrace\"},\"jobId\":\"SomeJobId\",\"cp\":[{\"appName\":\"SomeName\",\"binaryType\":\"Jar\",\"uploadTime\":\"" + earlyDateStr + "\",\"binaryStorageId\":\"SomeStorId\"},{\"appName\":\"SomeName\",\"binaryType\":\"Egg\",\"uploadTime\":\"" + earlyDateStr + "\",\"binaryStorageId\":null}],\"contextId\":\"SomeContextId\"}"
  val testJobInfoWithBinInfoJson = "{\"binaryInfo\":{\"appName\":\"SomeName\",\"binaryType\":\"Jar\",\"uploadTime\":\"" + earlyDateStr + "\",\"binaryStorageId\":\"SomeStorId\"},\"classPath\":\"SomeClassPath\",\"startTime\":\"" + earlyDateStr + "\",\"state\":\"SomeState\",\"contextName\":\"SomeContextName\",\"endTime\":\"" + dateStr + "\",\"error\":{\"message\":\"SomeMessage\",\"errorClass\":\"SomeClass\",\"stackTrace\":\"SomeTrace\"},\"jobId\":\"SomeJobId\",\"cp\":[],\"contextId\":\"SomeContextId\"}"
  // ContextInfo
  val testContextInfo = ContextInfo("someId", "someName", "someConfig", Some("ActorAddress"),
      earlyDate, Some(date), "someState", Some(new Throwable("message")))
  val testContextInfo2 = ContextInfo("someId", "someName", "someConfig", None, earlyDate, None,
      "someState", None)
  val testContextInfoJson = "{\"name\":\"someName\",\"actorAddress\":\"ActorAddress\",\"startTime\":\"" + earlyDateStr + "\",\"state\":\"someState\",\"config\":\"someConfig\",\"endTime\":\"" + dateStr + "\",\"id\":\"someId\",\"error\":\"message\"}"
  val testContextInfo2Json = "{\"name\":\"someName\",\"actorAddress\":null,\"startTime\":\"" + earlyDateStr + "\",\"state\":\"someState\",\"config\":\"someConfig\",\"endTime\":null,\"id\":\"someId\",\"error\":null}"

  /*
   * Test: BinaryInfo
   */

  describe("BinaryInfo") {

    it("should serialize BinaryInfo") {
      val serial = testBinaryInfo.toJson
      serial.compactPrint should equal(testBinaryInfoJson)
    }

    it("should deserialize BinaryInfo") {
      val deserial = testBinaryInfoJson.parseJson.convertTo[BinaryInfo]
      deserial should equal(testBinaryInfo)
    }

    it("should handle the absence of optional BinaryInfo values correctly") {
      val serial = testBinaryInfo2.toJson
      serial.compactPrint should equal(testBinaryInfo2Json)
      val deserial = testBinaryInfo2Json.parseJson.convertTo[BinaryInfo]
      deserial should equal(testBinaryInfo2)
    }

    it("should handle binary info for URIs") {
      val serial = testBinaryInfo3.toJson
      serial.compactPrint should equal(testBinaryInfo3Json)
      val deserial = testBinaryInfo3Json.parseJson.convertTo[BinaryInfo]
      deserial should equal(testBinaryInfo3)
    }
  }

  /*
   * Test: JobInfo
   */

  describe("JobInfo") {

    it("should serialize JobInfo") {
      val serial = testJobInfo.toJson
      serial.compactPrint should equal(testJobInfoJson)
    }

    it("should deserialize JobInfo") {
      val deserial = testJobInfoJson.parseJson.convertTo[JobInfo]
      deserial should equal(testJobInfo)
    }

    it("should serialize JobInfo with cp value set") {
      val serial = testJobInfoWithCp.toJson
      serial.compactPrint should equal(testJobInfoWithNonEmptyCpJson)
    }

    it("should handle the absence of optional JobInfo values correctly") {
      val serial = testJobInfo2.toJson
      serial.compactPrint should equal(testJobInfo2Json)
      val deserial = testJobInfo2Json.parseJson.convertTo[JobInfo]
      deserial should equal(testJobInfo2)
    }

    describe("should support legacy job info formats") {
      it("should deserialize old data format, where there was no cp parameter at all") {
        val deserial = testJobInfoNoCpJson.parseJson.convertTo[JobInfo]
        deserial should equal(testJobInfo)
      }

      it("should deserialize old data format, where binary info set not in cp") {
        val deserial = testJobInfoWithBinInfoJson.parseJson.convertTo[JobInfo]
        deserial should equal(testJobInfo)
      }
    }
  }

  /*
   * Test: ContextInfo
   */

  describe("ContextInfo") {

    it("should serialize ContextInfo") {
      val serial = testContextInfo.toJson
      serial.compactPrint should equal(testContextInfoJson)
    }

    it("should deserialize ContextInfo") {
      val deserial = testContextInfoJson.parseJson.convertTo[ContextInfo]
      val a = deserial
      val b = testContextInfo
      a should equal(b)
    }

    it("should handle the absence of optional ContextInfo values correctly") {
      val serial = testContextInfo2.toJson
      serial.compactPrint should equal(testContextInfo2Json)
      val deserial = testContextInfo2Json.parseJson.convertTo[ContextInfo]
      deserial should equal(testContextInfo2)
    }

  }
}