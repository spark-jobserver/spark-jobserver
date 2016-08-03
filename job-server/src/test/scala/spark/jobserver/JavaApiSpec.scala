package spark.jobserver

import akka.testkit._
import com.typesafe.config.ConfigFactory
import spark.jobserver.io.JobDAOActor
import spark.jobserver.CommonMessages._

import scala.concurrent.duration._
import java.util._

object JavaApiSpec extends JobSpecConfig {
  override val contextFactory = "spark.jobserver.context.JavaContextFactory"
}

class JavaApiSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {
  val apiConfig = ConfigFactory
    .parseString(
        """
      |context-factory = "spark.jobserver.context.JavaContextFactory"
      |context.name = "JavaApiCtx"
      |context.actorName = "JavaApiCtx"
    """.stripMargin)
    .withFallback(JavaApiSpec.config)

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(
        JobManagerActor.props(JavaApiSpec.getContextConfig(false))
    )
  }

  val classPrefix = "spark.jobserver."
  val testClass = classPrefix + "JavaJobTest"
  val wordCount = classPrefix + "WordCountJava"

  val initMsgWait = 20.seconds.dilated
  val startJobWait = 5.seconds.dilated

  val sentence = "scala scala java python python"
  val expectedCount = new HashMap[String, Int]()

  expectedCount.put("scala", 2)
  expectedCount.put("java", 1)
  expectedCount.put("python", 2)

  val stringConfig = ConfigFactory.parseString(s"input = $sentence")
  val emptyConfig = ConfigFactory.empty()

  describe("Java Context Factory") {

    it("Should Create Context and Run Test Job") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo",
                                         testClass,
                                         emptyConfig,
                                         errorEvents ++ syncEvents)
      expectMsgPF(3 seconds, "Expect a result or an error") {
        case JobResult(_, result) => result shouldBe 0
        case JobErroredOut(_, _, e: Throwable) => throw e
      }
    }
    it("Should run word count") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgPF(3 seconds, "Properly init") {
        case JobManagerActor.InitError(t) => throw t
        case JobManagerActor.Initialized(name, resultAct) =>
          name shouldBe "ctx"
      }
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo",
                                         wordCount,
                                         stringConfig,
                                         errorEvents ++ syncEvents)
      expectMsgPF(3 seconds, "Expect a result or an error") {
        case JobResult(_, result: Map[String, Int]) => result.get("scala") shouldBe 2
        case JobErroredOut(_, _, e: Throwable) => throw e
      }
    }
  }
}
