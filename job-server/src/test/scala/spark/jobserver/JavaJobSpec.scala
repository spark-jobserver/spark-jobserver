package spark.jobserver

import akka.testkit._
import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages.{JobErroredOut, JobResult}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.context.JavaSparkContextFactory
import spark.jobserver.io.JobDAOActor

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class JavaJobSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {

  val JobResultCacheSize = Integer.valueOf(30)
  val NumCpuCores = Integer.valueOf(Runtime.getRuntime.availableProcessors())
  val MemoryPerNode = "512m"
  val MaxJobsPerContext = Integer.valueOf(2)

  lazy val config = {
    val ConfigMap = Map(
      "spark.jobserver.job-result-cache-size" -> JobResultCacheSize,
      "num-cpu-cores" -> NumCpuCores,
      "memory-per-node" -> MemoryPerNode,
      "spark.jobserver.max-jobs-per-context" -> MaxJobsPerContext,
      "spark.jobserver.named-object-creation-timeout" -> "60 s",
      "akka.log-dead-letters" -> Integer.valueOf(0),
      "spark.master" -> "local[*]",
      "context-factory" -> contextFactory,
      "spark.context-settings.test" -> "",
      "context.name" -> "ctx",
      "context.actorname" -> "ctx",
      "is-adhoc" -> "false"
    )
    ConfigFactory.parseMap(ConfigMap.asJava).withFallback(ConfigFactory.defaultOverrides())
  }

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(config, daoActor))
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
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", javaJob, config, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "No job ever returned :'(") {
        case JobResult(_, result) => result should be("Hi!")
      }
    }
    it("Should fail running this java job"){
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", failedJob, config, errorEvents)
      expectMsgPF(6 seconds, "Gets correct exception"){
        case JobErroredOut(_, _, ex) => ex.getMessage should equal("java.lang.RuntimeException: fail")
      }
    }
  }
}
