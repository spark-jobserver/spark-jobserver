package spark.jobserver

import akka.testkit._
import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages.{JobErroredOut, JobFinished, JobStarted}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.context.JavaSparkContextFactory
import spark.jobserver.io.JobDAOActor.{GetJobResult, JobResult}
import spark.jobserver.io.{InMemoryBinaryObjectsDAO, InMemoryMetaDAO, JobDAOActor}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class JavaJobSpec extends JobSpecBase(JobManagerActorSpec.getNewSystem) {

  val NumCpuCores = Integer.valueOf(Runtime.getRuntime.availableProcessors())
  val MemoryPerNode = "512m"
  private val MaxJobsPerContext = Integer.valueOf(2)

  lazy val config = {
    val ConfigMap = Map(
      "num-cpu-cores" -> NumCpuCores,
      "memory-per-node" -> MemoryPerNode,
      "spark.jobserver.max-jobs-per-context" -> MaxJobsPerContext,
      "spark.jobserver.named-object-creation-timeout" -> "60 s",
      "akka.log-dead-letters" -> Integer.valueOf(0),
      "spark.master" -> "local[*]",
      "context-factory" -> contextFactory,
      "spark.context-settings.test" -> "",
      "context.name" -> "ctx",
      "is-adhoc" -> "false"
    )
    ConfigFactory.parseMap(ConfigMap.asJava).withFallback(ConfigFactory.defaultOverrides()).
      withFallback(ConfigFactory.parseString("cp = [\"demo\"]"))
  }

  before {
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryObjectsDAO
    daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
    manager = system.actorOf(JobManagerActor.props(daoActor))
    supervisor = TestProbe().ref
  }

  after {
    AkkaTestUtils.shutdownAndWait(manager)
  }

  def contextFactory: String = classOf[JavaSparkContextFactory].getName
  val classPrefix = "spark.jobserver."
  val javaJob = classPrefix + "JavaHelloWorldJob"
  val failedJob = classPrefix + "FailingJavaJob"
  val initMsgWait = 10.seconds.dilated
  val startJobWait = 10.seconds.dilated


  describe("Running Java Jobs") {
    it("Should run a java job") {
      val testJar = uploadTestJar()

      manager ! JobManagerActor.Initialize(config, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StartJob(javaJob, List(testJar), config, allEvents)
      expectMsgPF(startJobWait, "No job ever returned :'(") {
        case JobStarted(jobId, _jobInfo) =>
          expectMsgClass(classOf[JobFinished])
          daoActor ! GetJobResult(jobId)
          expectMsg(JobResult("Hi!"))
      }
    }

    it("Should fail running this java job"){
      val testJar = uploadTestJar()

      manager ! JobManagerActor.Initialize(config, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      manager ! JobManagerActor.StartJob(failedJob, List(testJar), config, errorEvents)
      expectMsgPF(6 seconds, "Gets correct exception"){
        case JobErroredOut(_, _, ex) => ex.getMessage should equal("fail")
      }
    }
  }
}
