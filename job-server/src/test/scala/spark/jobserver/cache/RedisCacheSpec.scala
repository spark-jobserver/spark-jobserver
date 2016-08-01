package spark.jobserver.cache

import com.typesafe.config.Config
import org.apache.commons.lang.SerializationUtils
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.scalactic.{Every, Good, Or}
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.JobJarInfo
import spark.jobserver.api.{JobEnvironment, SparkJobBase, ValidationProblem}

/**
  * Created by scarman on 7/30/16.
  */
object Testjob extends SparkJobBase {
  type C = SparkContext
  type JobData = Int
  type JobOutput = Int

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: Int): Int = {
    1
  }
  def validate(sc: SparkContext,
               runtime: JobEnvironment,
               config: Config): Int Or Every[ValidationProblem] = {
    Good(1)
  }
}

class RedisCacheSpec extends FunSpec with Matchers {
  val redis = new RedisCache[JobJarInfo[SparkJobBase]]("localhost", 6379)
  val dt = DateTime.now()
  val k = ("app", dt, "classpath.jar:tools.jar").toString()
  val serializedK = List("app", dt.toString, "classpath.jar:tools.jar")
  val v = JobJarInfo(() => Testjob, "TestJob", "/test.jar")
  val serializedV = SerializationUtils.serialize(v)

  describe("Encoding Keys and Values") {

    it("Should serialize to bytes") {
      redis.valueToBytes(v) should be (serializedV)
    }

    it("Should serialize from bytes") {
      redis.bytesToValue(serializedV) === v
    }

  }

  describe("Redis Client"){
    it("Should get size"){
      redis.size shouldBe 0
    }
    it("Should put value"){
      redis.put(k, v)
    }
    it("Should update cache size"){
      redis.size shouldBe 1
    }
    it("Should get value"){
      redis.get(k) === v
    }
    it("Should get or put"){
      redis.getOrPut(k, v) === v
    }
    it("Should contain"){
      redis.contains(k) shouldBe true
    }
  }

}
