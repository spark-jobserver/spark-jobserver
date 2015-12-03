package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.io.JobDAO

object JobInfoActorSpec {
  val system = ActorSystem("test")
}

class JobInfoActorSpec extends TestKit(JobInfoActorSpec.system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import CommonMessages.NoSuchJobId
  import JobInfoActor._
  import com.typesafe.config._

  private val jobId = "jobId"
  private val jobConfig = ConfigFactory.empty()

  override def afterAll() {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(JobInfoActorSpec.system)
  }

  var actor: ActorRef = _
  var dao: JobDAO = _

  before {
    dao = new InMemoryDAO
    actor = system.actorOf(Props(classOf[JobInfoActor], dao, system.actorOf(Props(classOf[LocalContextSupervisorActor], dao))))
  }

  after {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(actor)
  }

  describe("JobInfoActor") {
    it("should store a job configuration") {
      actor ! StoreJobConfig(jobId, jobConfig)
      expectMsg(JobConfigStored)
      dao.getJobConfigs.get(jobId) should be (Some(jobConfig))
    }

    it("should return a job configuration when the jobId exists") {
      actor ! StoreJobConfig(jobId, jobConfig)
      expectMsg(JobConfigStored)
      actor ! GetJobConfig(jobId)
      expectMsg(jobConfig)
    }

    it("should return error if jobId does not exist") {
      actor ! GetJobConfig(jobId)
      expectMsg(NoSuchJobId)
    }
  }
}
