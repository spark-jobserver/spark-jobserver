package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe, TestActor}
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
  private val contextId = "contextId"
  private val contextName = "contextName"
  private val appName = "appName"
  private val jarInfo = BinaryInfo(appName, BinaryType.Jar, DateTime.now)
  private val classPath = "classPath"
  private val jobInfo = JobInfo(jobId, contextId, contextName, jarInfo, classPath, JobStatus.Running,
      DateTime.now, None, None)
  private val endTime = DateTime.now()
  private val error = new Throwable

  override def afterAll() {
    akka.AkkaTestUtils.shutdownAndWait(JobStatusActorSpec.system)
  }

  var actor: ActorRef = _
  var receiver: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _
  var daoProbe: TestProbe = _
  var daoMsgReceiverProbe: TestProbe = _

  before {
    daoProbe = TestProbe()
    daoMsgReceiverProbe = TestProbe()
    daoProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
        sender ! "All good"
        daoMsgReceiverProbe.ref ! msg
        TestActor.KeepRunning
      }
    })

    daoActor = daoProbe.ref
    actor = system.actorOf(Props(classOf[JobStatusActor], daoActor))
    receiver = system.actorOf(Props[JobResultActor])
  }

  after {
    AkkaTestUtils.shutdownAndWait(actor)
  }

  describe("JobStatusActor") {
    it("should return empty sequence if there is no job infos") {
      actor ! GetRunningJobStatus
      daoMsgReceiverProbe.expectNoMsg()
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
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo))
      expectMsg(msg)

      actor ! msg
      expectMsg(msg)

      actor ! Unsubscribe(jobId, self)
      actor ! JobStarted(jobId, jobInfo)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo))
      expectNoMsg()   // shouldn't get it again

      actor ! Unsubscribe(jobId, self)
      expectMsg(NoSuchJobId)
    }

    it("should be ok to subscribe before job init") {
      actor ! Subscribe(jobId, self, Set(classOf[JobStarted]))
      actor ! JobInit(jobInfo)
      val msg = JobStarted(jobId, jobInfo)
      actor ! msg
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo))
      expectMsg(msg)
    }

    it("should be informed JobValidationFailed once") {
      actor ! JobInit(jobInfo)
      actor ! Subscribe(jobId, self, Set(classOf[JobValidationFailed]))
      val msg = JobValidationFailed(jobId, endTime, error)

      actor ! msg
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo.copy(
          state = JobStatus.Error, endTime = Some(endTime), error = Some(ErrorData(error)))))
      expectMsg(msg)

      actor ! msg
      daoMsgReceiverProbe.expectNoMsg()
      expectMsg(NoSuchJobId)
    }

    it("should be informed JobFinished until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! JobStarted(jobId, jobInfo)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo))
      actor ! Subscribe(jobId, self, Set(classOf[JobFinished]))

      val msg = JobFinished(jobId, endTime)
      actor ! msg
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo.copy(
          state = JobStatus.Finished, endTime = Some(endTime))))
      expectMsg(msg)

      actor ! msg
      daoMsgReceiverProbe.expectNoMsg()
      expectMsg(NoSuchJobId)
    }

    it("should be informed JobErroredOut until it is unsubscribed") {
      actor ! JobInit(jobInfo)
      actor ! JobStarted(jobId, jobInfo)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo))
      actor ! Subscribe(jobId, self, Set(classOf[JobErroredOut]))

      val msg = JobErroredOut(jobId, endTime, error)
      actor ! msg
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo.copy(
          state = JobStatus.Error, endTime = Some(endTime), error = Some(ErrorData(error)))))
      expectMsg(msg)

      actor ! msg
      daoMsgReceiverProbe.expectNoMsg()
      expectMsg(NoSuchJobId)
    }

    it("should update status correctly") {
      actor ! JobInit(jobInfo)
      actor ! GetRunningJobStatus
      expectMsg(Seq(jobInfo))

      val jobInfoWithStartTime = jobInfo.copy(startTime=DateTime.now)
      actor ! JobStarted(jobId, jobInfoWithStartTime)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfoWithStartTime))

      actor ! GetRunningJobStatus
      expectMsg(Seq(jobInfoWithStartTime))

      val finishTime = DateTime.now
      actor ! JobFinished(jobId, finishTime)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfoWithStartTime.copy(
          state = JobStatus.Finished, endTime = Some(finishTime))))

      actor ! GetRunningJobStatus
      daoMsgReceiverProbe.expectNoMsg()
      expectMsg(Seq.empty)
    }

    it("should update JobValidationFailed status correctly") {
      val jobInfoWithInitTime = jobInfo.copy(startTime=DateTime.now)
      actor ! JobInit(jobInfoWithInitTime)

      actor ! JobValidationFailed(jobId, endTime, error)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfoWithInitTime.copy(
          state = JobStatus.Error, endTime = Some(endTime), error = Some(ErrorData(error)))))

      actor ! GetRunningJobStatus
      daoMsgReceiverProbe.expectNoMsg()
      expectMsg(Seq.empty)
    }

    it("should update JobErroredOut status correctly") {
      actor ! JobInit(jobInfo)

      actor ! JobStarted(jobId, jobInfo)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo))

      actor ! JobErroredOut(jobId, endTime, error)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo.copy(
          state = JobStatus.Error, endTime = Some(endTime), error = Some(ErrorData(error)))))

      actor ! GetRunningJobStatus
      daoMsgReceiverProbe.expectNoMsg()
      expectMsg(Seq.empty)
    }

    it("should update JobKilled status correctly") {
      import spark.jobserver.JobManagerActor.JobKilledException
      import scala.concurrent.duration._
      actor ! JobInit(jobInfo)

      actor ! JobStarted(jobId, jobInfo)
      daoMsgReceiverProbe.expectMsg(JobDAOActor.SaveJobInfo(jobInfo))

      actor ! JobKilled(jobId, endTime)
      val id = daoMsgReceiverProbe.expectMsgPF(3 seconds, "We cannot pass custom throwable inside JobKilled") {
        case JobDAOActor.SaveJobInfo(receivedJobInfo) =>
          receivedJobInfo.jobId should be (jobId)
          receivedJobInfo.state should be (JobStatus.Killed)
          receivedJobInfo.endTime should be (Some(endTime))
          receivedJobInfo.error.get.message should be (s"Job $jobId killed")
      }

      actor ! GetRunningJobStatus
      daoMsgReceiverProbe.expectNoMsg()
      expectMsg(Seq.empty)
    }
  }
}
