package spark.jobserver

import akka.testkit._
import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages._
import spark.jobserver.io.JobDAOActor

import scala.concurrent.duration._

object JavaApiSpec extends JobSpecConfig {
  override val contextFactory = "spark.jobserver.context.JavaContextFactory"
}

class JavaApiSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(
        JobManagerActor.props(JavaApiSpec.getContextConfig(false))
    )
  }

  val classPrefix = "spark.jobserver."
  val testClass = classPrefix + "examples.JavaJobTest"
  val failValidation = classPrefix + "examples.FailValidationJob"
  val failRunJob = classPrefix + "examples.FailRunJob"
  val wordCount = classPrefix + "WordCountJava"

  val initMsgWait = 20.seconds.dilated
  val startJobWait = 5.seconds.dilated
  val jobWait = 3.seconds.dilated

  val sentence = "scala scala java python python"

  val stringConfig = ConfigFactory.parseString(s"input = $sentence")
  val emptyConfig = ConfigFactory.empty()

  describe("Java Api") {

    it("Should Create Context and Run Test Job") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", testClass, emptyConfig, errorEvents ++ syncEvents)
      expectMsgPF(jobWait, "Expect a result or an error") {
        case JobResult(_, result) => result shouldBe 0
        case JobErroredOut(_, _, e: Throwable) => throw e
      }
    }

    it("Should fail validation"){
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", failValidation, emptyConfig, errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobValidationFailed])
    }

    it("Should fail run job"){
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", failRunJob, emptyConfig, errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobErroredOut])
    }

    it("Should run word count") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCount, stringConfig, errorEvents ++ syncEvents)
      expectMsgPF(jobWait, "Expect a result or an error") {
        case JobResult(_, result: java.util.Map[String, Int]) => {
          result.get("scala") shouldBe 2
          result.get("java") shouldBe 1
          result.get("python") shouldBe 2
        }
        case JobErroredOut(_, _, e: Throwable) => throw e
      }
    }
  }
}
