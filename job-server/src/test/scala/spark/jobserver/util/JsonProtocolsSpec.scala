package spark.jobserver.util

import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spark.jobserver.io.{BinaryInfo, BinaryType, ContextInfo, JobInfo}
import spark.jobserver.util.JsonProtocols._
import spray.json._

import java.time.{ZoneId, ZonedDateTime}
import java.util.TimeZone

class JsonProtocolsSpec extends AnyFunSpec with Matchers with BeforeAndAfter {

  /*
   * Test data
   */

  // Date formatting
  val earlyDate = ZonedDateTime.of(2021, 5, 11, 12, 15, 0, 250000000, ZoneId.of("Europe/Berlin"))
  val date = ZonedDateTime.of(2021, 5, 11, 13, 15, 0, 250000000, ZoneId.of("Europe/Berlin"))
  val earlyDateStr = "2021-05-11T12:15:00.250+02:00"
  val dateStr = "2021-05-11T13:15:00.250+02:00"
  val legacyDateStr = "2021-05-11 13-15-00 250 +0200"

  // BinaryInfo
  val testBinaryInfo = BinaryInfo("SomeName", BinaryType.Jar, earlyDate, Some("SomeStorId"))
  val testBinaryInfoJson = f"""{"appName":"SomeName","binaryStorageId":"SomeStorId","binaryType":"Jar","uploadTime":"${earlyDateStr}"}"""
  val testBinaryInfo2 = BinaryInfo("SomeName", BinaryType.Egg, earlyDate, None)
  val testBinaryInfo2Json = f"""{"appName":"SomeName","binaryStorageId":null,"binaryType":"Egg","uploadTime":"${earlyDateStr}"}"""
  val testBinaryInfo3 = BinaryInfo("file://some/path/to/file", BinaryType.URI, earlyDate, None)
  val testBinaryInfo3Json = f"""{"appName":"file://some/path/to/file","binaryStorageId":null,"binaryType":"Uri","uploadTime":"${earlyDateStr}"}"""

  // JobInfo
  val testJobInfo = JobInfo("SomeJobId", "SomeContextId", "SomeContextName",
    "SomeClassPath", "SomeState", earlyDate, Some(date), Some(ErrorData("SomeMessage",
      "SomeClass", "SomeTrace")), Seq.empty, None)
  val testJobInfo2 = JobInfo("SomeJobId", "SomeContextId", "SomeContextName",
    "SomeClassPath", "SomeState", earlyDate, None, None, Seq.empty, None)
  val testJobInfoWithCp = JobInfo("SomeJobId", "SomeContextId", "SomeContextName",
    "SomeClassPath", "SomeState", earlyDate, Some(date), Some(ErrorData("SomeMessage",
      "SomeClass", "SomeTrace")), Seq(testBinaryInfo, testBinaryInfo2), None)
  val testJobInfoJson = f"""{"classPath":"SomeClassPath","contextId":"SomeContextId","contextName":"SomeContextName","cp":[],"endTime":"${dateStr}","error":{"errorClass":"SomeClass","message":"SomeMessage","stackTrace":"SomeTrace"},"jobId":"SomeJobId","startTime":"${earlyDateStr}","state":"SomeState"}"""
  val testJobInfoNoCpJson = f"""{"classPath":"SomeClassPath","contextId":"SomeContextId","contextName":"SomeContextName","endTime":"${dateStr}","error":{"errorClass":"SomeClass","message":"SomeMessage","stackTrace":"SomeTrace"},"jobId":"SomeJobId","startTime":"${earlyDateStr}","state":"SomeState"}"""
  val testJobInfo2Json = f"""{"classPath":"SomeClassPath","contextId":"SomeContextId","contextName":"SomeContextName","cp":[],"endTime":null,"error":null,"jobId":"SomeJobId","startTime":"${earlyDateStr}","state":"SomeState"}"""
  val testJobInfoWithNonEmptyCpJson = f"""{"classPath":"SomeClassPath","contextId":"SomeContextId","contextName":"SomeContextName","cp":[{"appName":"SomeName","binaryStorageId":"SomeStorId","binaryType":"Jar","uploadTime":"${earlyDateStr}"},{"appName":"SomeName","binaryStorageId":null,"binaryType":"Egg","uploadTime":"${earlyDateStr}"}],"endTime":"${dateStr}","error":{"errorClass":"SomeClass","message":"SomeMessage","stackTrace":"SomeTrace"},"jobId":"SomeJobId","startTime":"${earlyDateStr}","state":"SomeState"}"""
  val testJobInfoWithBinInfoJson = f"""{"binaryInfo":{"appName":"SomeName","binaryType":"Jar","uploadTime":"${earlyDateStr}","binaryStorageId":"SomeStorId"},"classPath":"SomeClassPath","contextId":"SomeContextId","contextName":"SomeContextName","cp":[],"endTime":"${dateStr}","error":{"message":"SomeMessage","errorClass":"SomeClass","stackTrace":"SomeTrace"},"jobId":"SomeJobId","startTime":"${earlyDateStr}","state":"SomeState"}"""
  // ContextInfo
  val testContextInfo = ContextInfo("someId", "someName", "someConfig", Some("ActorAddress"),
    earlyDate, Some(date), "someState", Some(new Throwable("message")))
  val testContextInfo2 = ContextInfo("someId", "someName", "someConfig", None, earlyDate, None,
    "someState", None)
  val testContextInfo3 = ContextInfo("someId", "someName", "someConfig", None, date, None,
    "someState", None)
  val testContextInfoJson = f"""{"actorAddress":"ActorAddress","config":"someConfig","endTime":"${dateStr}","error":"message","id":"someId","name":"someName","startTime":"${earlyDateStr}","state":"someState"}"""
  val testContextInfo2Json = f"""{"actorAddress":null,"config":"someConfig","endTime":null,"error":null,"id":"someId","name":"someName","startTime":"${earlyDateStr}","state":"someState"}"""
  val testContextInfo3Json = f"""{"actorAddress":null,"config":"someConfig","endTime":null,"error":null,"id":"someId","name":"someName","startTime":"${legacyDateStr}","state":"someState"}"""

  private val originalTimezone = TimeZone.getDefault

  before {
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"))
  }

  after {
    TimeZone.setDefault(originalTimezone)
  }

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

    it("should deserialize ContextInfo with old timestamps") {
      val deserial = testContextInfo3Json.parseJson.convertTo[ContextInfo]
      val a = deserial
      val b = testContextInfo3
      a should equal(b)
    }

    it("should handle the absence of optional ContextInfo values correctly") {
      val serial = testContextInfo2.toJson
      serial.compactPrint should equal(testContextInfo2Json)
      val deserial = testContextInfo2Json.parseJson.convertTo[ContextInfo]
      deserial should equal(testContextInfo2)
    }

  }

  describe("Joda Time") {
    it("should serialize a UTC timestamp like Joda") {
      val date = ZonedDateTime.of(2015, 10, 16, 3, 17, 3, 127000000, ZoneId.of("UTC"))
      JsonProtocols.fromDateTime(date) should equal("2015-10-16T03:17:03.127Z")
    }

    it("should serialize a timestamp with timezone like Joda") {
      val date1 = ZonedDateTime.of(2016, 6, 19, 16, 27, 12, 196000000, ZoneId.of("+05:30"))
      JsonProtocols.fromDateTime(date1) should equal("2016-06-19T16:27:12.196+05:30")

      val date2 = ZonedDateTime.of(2016, 8, 1, 15, 15, 52, 250000000, ZoneId.of("+01:00"))
      JsonProtocols.fromDateTime(date2) should equal("2016-08-01T15:15:52.250+01:00")
    }
  }
}