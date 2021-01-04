package spark.jobserver.util

import akka.http.scaladsl.model.Uri

import java.io.File
import org.scalatest.{FunSpecLike, Matchers}
import spark.jobserver.JobManagerActor.StartJob
import spark.jobserver.CommonMessages.{JobErroredOut, JobValidationFailed}
import com.typesafe.config._
import org.joda.time.DateTime
import spark.jobserver.io.{BinaryInfo, BinaryType}

class StartJobSerializerSpec extends FunSpecLike with Matchers {

  val binInfo: BinaryInfo = BinaryInfo("name", BinaryType.Jar, DateTime.now(), None)

  describe("StartJobSerializer") {
    it("should match StartJob after serialize and deserialize") {
      val binInfo = BinaryInfo("name", BinaryType.Jar, DateTime.now(), None)
      val startJob = StartJob("spark.jobserver.test.ClassPath", List(binInfo),
        ConfigFactory.parseString("a=2"), Set(classOf[JobErroredOut], classOf[JobValidationFailed]),
        None, Some(Uri("http://example.com")))
      val serializer = new StartJobSerializer()
      val obj = serializer.fromBinaryJava(serializer.toBinary(startJob), null)
      obj.isInstanceOf[StartJob].shouldBe(true)
      obj.asInstanceOf[StartJob].cp.shouldBe(List(binInfo))
      obj.asInstanceOf[StartJob].config.getInt("a").shouldBe(2)
      obj.asInstanceOf[StartJob].existingJobInfo.shouldBe(None)
      obj.asInstanceOf[StartJob].callbackUrl.shouldBe(Some(Uri("http://example.com")))
    }

    it("should match StartJob - large config value") {
      val config = ConfigFactory.parseFile(
        new File(getClass.getClassLoader.getResource("startjobSerializerConfig.conf").getPath))
      val startJob = StartJob("spark.jobserver.test.ClassPath", List(binInfo),
        config, Set(classOf[JobErroredOut], classOf[JobValidationFailed]),
        None, Some(Uri("http://example.com")))
      val serializer = new StartJobSerializer()
      val obj = serializer.fromBinaryJava(serializer.toBinary(startJob), null)
      obj.isInstanceOf[StartJob] shouldBe true
      obj.asInstanceOf[StartJob].config.getInt("input").shouldEqual(1)
      obj.asInstanceOf[StartJob].config.getString("data").shouldBe(config.getString("data"))
      obj.asInstanceOf[StartJob].existingJobInfo.shouldBe(None)
      obj.asInstanceOf[StartJob].callbackUrl.shouldBe(Some(Uri("http://example.com")))
    }

    it("should catch IllegalArgumentException raised from Unknown object serialization") {
      intercept[IllegalArgumentException] {
        case class Crash(s: String, i: Int)
        val serializer = new StartJobSerializer()
        val obj = serializer.toBinary(Crash("hello", 1))
      }
    }
  }
}
