package spark.jobserver

import scala.concurrent.duration._

import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.JobDAOActor
import scala.collection.JavaConverters._

import org.scalactic.Good
import spark.jobserver.context.JavaToScalaWrapper

object JavaApiSpec extends JobSpecConfig {
  //override val contextFactory = classOf[JavaSparkContextFactory].getName
}

class JavaApiSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {

  val classPrefix = "spark.jobserver."
  val wordCountClass = classPrefix + "JavaJobTest"

  val initMsgWait = 20.seconds.dilated
  val startJobWait = 5.seconds.dilated

  val sentence = "The quick brown fox jumped over the lazy dog dog"
  val stringConfig = ConfigFactory.parseString(s"input.string = $sentence")
  val emptyConfig = ConfigFactory.empty()

  lazy val cc = {
    val ConfigMap = Map(
      "context-factory" -> classOf[JavaSparkContextFactory].getName,
      "context.name" -> "ctx",
      "context.actorname" -> "ctx",
      "is-adhoc" -> true
    )
    ConfigFactory.parseMap(ConfigMap.asJava).withFallback(ConfigFactory.defaultOverrides())
  }

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(this.cc))
  }

  after {
    AkkaTestUtils.shutdownAndWait(manager)
  }

  describe("Java API") {

    it("Should blow up because ??") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, emptyConfig, allEvents)
      val JobResult(jobId: String, sum: Any) = expectMsgClass(classOf[JobResult])
      println(s"$jobId, $sum")
    }

    it("Should wrap a Java Api Job"){
      val conf = new SparkConf().setAppName("Java Api Test").setMaster("local[1]")
      val ctx = new JavaSparkContext(conf)
      val noop = new JavaJobTest()
      val wrapper = new JavaToScalaWrapper(noop)
      wrapper.runJob(ctx.sc.asInstanceOf[wrapper.C], null, 0) shouldBe 0
      wrapper.validate(ctx.sc.asInstanceOf[wrapper.C], null, emptyConfig) shouldBe Good(1)
    }

  }
}
