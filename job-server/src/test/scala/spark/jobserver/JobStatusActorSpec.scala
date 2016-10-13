package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import spark.jobserver.io._
import org.joda.time.DateTime
import org.scalatest.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike}
import spark.jobserver.common.akka
import spark.jobserver.common.akka.AkkaTestUtils

object JobStatusActorSpec {
  val system = ActorSystem("test")
}

class JobStatusActorSpec extends TestKit(JobStatusActorSpec.system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import CommonMessages._
  import JobStatusActor._

  private val jobId = "jobId"
  private val contextName = "contextName"
  private val appName = "appName"
  private val jarInfo = BinaryInfo(appName, BinaryType.Jar, DateTime.now)
  private val classPath = "classPath"
  private val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, DateTime.now, None, None)

  override def afterAll() {
    akka.AkkaTestUtils.shutdownAndWait(JobStatusActorSpec.system)
  }

  var actor: ActorRef = _
  var receiver: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    actor = system.actorOf(Props(classOf[JobStatusActor], daoActor))
    receiver = system.actorOf(Props[JobResultActor])
  }

  after {
    AkkaTestUtils.shutdownAndWait(actor)
  }

  describe("JobStatusActor") {
    it("should return empty sequence if there is no job infos") {
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }

    it("should return error if non-existing job is unsubscribed") {
      actor ! Unsubscribe(jobId, receiver)
      expectMsg(NoSuchJobId)
    }

    it("should not initialize a job more than two times") {
      actor ! JobInit(jobInfo)
      actor ! JobInit(jobInfo)
      expectMsg(JobInitAlready)
    }

    it("should be informed JobStarted until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! Subscribe(jobId, self, Set(classOf[JobStarted]))
      val msg = JobStarted(jobId, jobInfo)
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(msg)

      actor ! Unsubscribe(jobId, self)
      actor ! JobStarted(jobId, jobInfo)
      expectNoMsg()   // shouldn't get it again

      actor ! Unsubscribe(jobId, self)
      expectMsg(NoSuchJobId)
    }

    it("should be ok to subscribe beofore job init") {
      actor ! Subscribe(jobId, self, Set(classOf[JobStarted]))
      actor ! JobInit(jobInfo)
      val msg = JobStarted(jobId, jobInfo)
      actor ! msg
      expectMsg(msg)
    }

    it("should be informed JobValidationFailed once") {
      actor ! JobInit(jobInfo)
      actor ! Subscribe(jobId, self, Set(classOf[JobValidationFailed]))
      val msg = JobValidationFailed(jobId, DateTime.now, new Throwable)
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(NoSuchJobId)
    }

    it("should be informed JobFinished until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! JobStarted(jobId, jobInfo)
      actor ! Subscribe(jobId, self, Set(classOf[JobFinished]))
      val msg = JobFinished(jobId, DateTime.now)
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(NoSuchJobId)
    }

    it("should be informed JobErroredOut until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! JobStarted(jobId, jobInfo)
      actor ! Subscribe(jobId, self, Set(classOf[JobErroredOut]))
      val msg = JobErroredOut(jobId, DateTime.now, new Throwable)
      actor ! msg
      expectMsg(msg)

      actor ! msg
      expectMsg(NoSuchJobId)
    }

    it("should update status correctly") {
      actor ! JobInit(jobInfo)
      actor ! GetRunningJobStatus
      expectMsg(Seq(jobInfo))

      val startTime = DateTime.now
      actor ! JobStarted(jobId, jobInfo.copy(startTime=startTime))
      actor ! GetRunningJobStatus
      expectMsg(Seq(JobInfo(jobId, contextName, jarInfo, classPath, startTime, None, None)))

      val finishTIme = DateTime.now
      actor ! JobFinished(jobId, finishTIme)
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }

    it("should update JobValidationFailed status correctly") {
      val initTime = DateTime.now
      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, initTime, None, None)
      actor ! JobInit(jobInfo)

      val failedTime = DateTime.now
      val err = new Throwable
      actor ! JobValidationFailed(jobId, failedTime, err)
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }

    it("should update JobErroredOut status correctly") {
      actor ! JobInit(jobInfo)

      val startTime = DateTime.now
      actor ! JobStarted(jobId, jobInfo)

      val failedTime = DateTime.now
      val err = new Throwable
      actor ! JobErroredOut(jobId, failedTime, err)
      actor ! GetRunningJobStatus
      expectMsg(Seq.empty)
    }
  }
}
