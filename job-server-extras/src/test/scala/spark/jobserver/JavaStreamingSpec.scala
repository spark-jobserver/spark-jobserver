package spark.jobserver

import akka.actor._
import akka.testkit._
import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages._
import spark.jobserver.context.JavaStreamingContextFactory
import spark.jobserver.io.{JobDAOActor, JobInfo}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object JavaStreamingSpec extends JobSpecConfig {
  override val contextFactory = classOf[JavaStreamingContextFactory].getName
}

class JavaStreamingSpec extends ExtrasJobSpecBase(JavaStreamingSpec.getNewSystem) {

  private val emptyConfig = ConfigFactory.parseMap(Map("streaming.batch_interval" -> 3).asJava)
  private val classPrefix = "spark.jobserver."
  private val streamingJob = classPrefix + "JStreamingTestJob"

  private def cfg = JavaStreamingSpec.getContextConfig(false, JavaStreamingSpec.contextConfig)

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(cfg, daoActor))
    supervisor = TestProbe().ref
  }

  describe("Running Java based Streaming Jobs") {
    it("Should return Correct results") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(10 seconds, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", streamingJob, emptyConfig, asyncEvents ++ errorEvents)
      val id = expectMsgPF(6 seconds, "No?") {
        case JobStarted(jid, _) =>
          jid should not be null
          jid
      }
      Thread.sleep(1000)
      val info = Await.result(dao.getJobInfo(id), 60 seconds)
      info.get match {
        case JobInfo(_, _, _, _, _, None, _) => {}
        case e => fail(s":-( No worky work $e")
      }
    }
  }
}
