package spark.jobserver

import scala.concurrent.duration._

import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.scalactic.{Good, Or}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.context._
import spark.jobserver.io.JobDAOActor

object JavaApiSpec extends JobSpecConfig {
  //override val contextFactory = classOf[JavaSparkContextFactory].getName
}

class JavaApiSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {
  val conf = new SparkConf().setAppName("Java Api Test").setMaster("local[1]")
  val ctx = new JavaSparkContext(conf)
  val classPrefix = "spark.jobserver."
  val wordCountClass = classPrefix + "WordCountJava"

  val initMsgWait = 20.seconds.dilated
  val startJobWait = 5.seconds.dilated

  val sentence = "The quick brown fox jumped over the lazy dog dog"
  val stringConfig = ConfigFactory.parseString(s"input.string = $sentence")
  val emptyConfig = ConfigFactory.empty()

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
            JavaApiSpec.getContextConfig(adhoc = false, JavaApiSpec.contextConfig)))
  }

  after {
    AkkaTestUtils.shutdownAndWait(manager)
  }

  describe("Java API") {

    it("Should blow up because ??") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, emptyConfig, allEvents)
    }

    it("Should wrap a Java Api Job"){
      val noop = new JavaJobTest()
      val wrapper = new JavaToScalaWrapper(noop)
      wrapper.runJob(ctx, null, 0) shouldBe 0
      wrapper.validate(ctx, null, emptyConfig) shouldBe Good(1)
    }

  }
}
