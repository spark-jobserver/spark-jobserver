package spark.jobserver.util

import java.io.File

import org.scalatest.{FunSpecLike, Matchers}
import spark.jobserver.JobManagerActor.StartJob
import spark.jobserver.CommonMessages.{JobErroredOut, JobValidationFailed}
import com.typesafe.config._

class StartJobSerializerSpec extends FunSpecLike with Matchers {

  describe("StartJobSerializer") {
    it("should match StartJob after serailize and desserailize") {
      val startJob = StartJob("testAppName", "spark.jobserver.test.ClassPath",
        ConfigFactory.parseString("a=2"), Set(classOf[JobErroredOut], classOf[JobValidationFailed]))
      val serializer = new StartJobSerializer()
      val obj = serializer.fromBinaryJava(serializer.toBinary(startJob), null)
      obj.isInstanceOf[StartJob].shouldBe(true)
      obj.asInstanceOf[StartJob].appName.shouldBe("testAppName")
      obj.asInstanceOf[StartJob].config.getInt("a").shouldBe(2)
    }

    it("should match StartJob - large config value") {
      val config = ConfigFactory.parseFile(
        new File(getClass.getClassLoader.getResource("startjobSerializerConfig.conf").getPath))
      val startJob = StartJob("testAppName", "spark.jobserver.test.ClassPath",
        config, Set(classOf[JobErroredOut], classOf[JobValidationFailed]))
      val serializer = new StartJobSerializer()
      val obj = serializer.fromBinaryJava(serializer.toBinary(startJob), null)
      obj.isInstanceOf[StartJob] shouldBe true
      obj.asInstanceOf[StartJob].config.getInt("input").shouldEqual(1)
      obj.asInstanceOf[StartJob].config.getString("data").shouldBe(config.getString("data"))
    }

    it("should catch IllegalArgumentException raised from Unknown object serialization") {
      intercept[IllegalArgumentException] {
        case class Crash(s:String, i:Int)
        val serializer = new StartJobSerializer()
        val obj = serializer.toBinary(Crash("hello", 1))
      }
    }
  }
}
