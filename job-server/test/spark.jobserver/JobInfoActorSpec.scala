package spark.jobserver

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.testkit.{TestKit, ImplicitSender}
import org.scalatest.{FunSpecLike, BeforeAndAfter, BeforeAndAfterAll, Matchers}

import spark.jobserver.io.{JobDAOActor, JobDAO}

object JobInfoActorSpec {
  val system = ActorSystem("test")
}

class JobInfoActorSpec extends TestKit(JobInfoActorSpec.system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import com.typesafe.config._
  import CommonMessages.NoSuchJobId
  import JobInfoActor._

  private val jobId = "jobId"
  private val jobConfig = ConfigFactory.empty()

  override def afterAll() {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(JobInfoActorSpec.system)
  }

  var actor: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    val supervisor = system.actorOf(Props(classOf[LocalContextSupervisorActor], daoActor))
    actor = system.actorOf(Props(classOf[JobInfoActor], dao, supervisor))
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
