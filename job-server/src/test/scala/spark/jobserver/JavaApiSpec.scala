package spark.jobserver

import akka.testkit._
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object JavaApiSpec extends JobSpecConfig

class JavaApiSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {

  val classPrefix = "spark.jobserver."
  val wordCountClass = classPrefix + "JavaJobTest"

  val initMsgWait = 20.seconds.dilated
  val startJobWait = 5.seconds.dilated

  val sentence = "The quick brown fox jumped over the lazy dog dog"
  val stringConfig = ConfigFactory.parseString(s"input.string = $sentence")
  val emptyConfig = ConfigFactory.empty()

  describe("Java API") {

    it("Should blow up because ??") {

    }
  }
}
