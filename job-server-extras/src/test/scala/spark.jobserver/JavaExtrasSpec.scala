package spark.jobserver

import akka.testkit._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import spark.jobserver.CommonMessages._
import spark.jobserver.JobManagerActor.{InitError, Initialized}
import spark.jobserver.context.{JavaSqlContextFactory, JavaStreamingContextFactory, SQLContextFactory}
import spark.jobserver.io.{JobDAOActor, JobInfo}

import scala.concurrent.Await
import scala.concurrent.duration._

object JavaExtrasSpec extends JobSpecConfig {

}

class JavaExtrasSpec extends ExtrasJobSpecBase(JavaExtrasSpec.getNewSystem) {
  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
      JavaExtrasSpec.getContextConfig(false, JavaExtrasSpec.contextConfig)))
  }
  val base = "spark.jobserver.examples."
  val testSqlJob = base + "SqlTestJob"
  val testStreamingJob = base + "StreamingTestJob"
  val initMsgWait = 20.seconds.dilated
  val startJobWait = 5.seconds.dilated
  val jobWait = 3.seconds.dilated

  val emptyConfig = ConfigFactory.empty()

  val sqlContext = JavaExtrasSpec.getContextConfig(false, JavaExtrasSpec.contextConfig)
    .withValue("context-factory", ConfigValueFactory.fromAnyRef(classOf[JavaSqlContextFactory].getName))

  val streamingContext = JavaExtrasSpec.getContextConfig(false, JavaExtrasSpec.contextConfig)
    .withValue("context-factory", ConfigValueFactory.fromAnyRef(classOf[JavaStreamingContextFactory].getName))

  describe("Java Extras Jobs") {
    it("Should run an SQL Job") {
      manager = system.actorOf(JobManagerActor.props(sqlContext))
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", testSqlJob, emptyConfig, errorEvents ++ syncEvents)
      expectMsgPF(jobWait, "Should be SQL Job Results") {
        case JobResult(_, result: Long) => result shouldBe 99L
      }
    }
    it("Should run a Streaming Job"){
      manager = system.actorOf(JobManagerActor.props(streamingContext))
      manager ! JobManagerActor.Initialize(daoActor, None)

      expectMsgPF(6 seconds){
        case j: Initialized => j.contextName shouldBe "ctx"
        case j: InitError => throw j.t
      }
      //expectMsgClass(classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", testStreamingJob, emptyConfig, asyncEvents ++ syncEvents)
      val jobId = expectMsgPF(6 seconds, "Did not start StreamingTestJob, expecting JobStarted") {
        case JobStarted(jobid, _) => {
          jobid should not be null
          jobid
        }
      }
      Thread sleep 1000
      val jobInfo = Await.result(dao.getJobInfo(jobId), 60 seconds)
      jobInfo.get match  {
        case JobInfo(_, _, _, _, _, None, _) => {  }
        case e => fail("Unexpected JobInfo" + e)
      }
    }
  }
}
