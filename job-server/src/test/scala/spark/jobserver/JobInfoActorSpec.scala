package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import org.joda.time.DateTime
import scala.concurrent._
import scala.concurrent.duration._

import spark.jobserver.common.akka
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io._

object JobInfoActorSpec {
  val system = ActorSystem("test")
}

class JobInfoActorSpec extends TestKit(JobInfoActorSpec.system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import com.typesafe.config._
  import CommonMessages.{NoSuchJobId, GetJobResult}
  import JobInfoActor._

  private val jobId = "jobId"
  private val jobConfig = ConfigFactory.empty()

  override def afterAll() {
    akka.AkkaTestUtils.shutdownAndWait(JobInfoActorSpec.system)
  }

  var actor: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _
  var dataManager: ActorRef = _

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    dataManager = system.actorOf(Props.empty)
    val supervisor = system.actorOf(Props(classOf[LocalContextSupervisorActor], daoActor, dataManager))
    actor = system.actorOf(Props(classOf[JobInfoActor], dao, supervisor))
  }

  after {
    AkkaTestUtils.shutdownAndWait(actor)
  }

  describe("JobInfoActor") {
    it("should store a job configuration") {
      actor ! StoreJobConfig(jobId, jobConfig)
      expectMsg(10 second, JobConfigStored)
      val storedJobConfig = Await.result(dao.getJobConfig(jobId), 60 seconds)
      storedJobConfig should be (Some(jobConfig))
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

    it("should return job info when requested for jobId that exists") {
      val dt = DateTime.parse("2013-05-29T00Z")
      val jobInfo = JobInfo(
        "foo", "cid", "context",
        "com.abc.meme", JobStatus.Running, dt, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt)))
      dao.saveJobInfo(jobInfo)
      actor ! GetJobStatus("foo")
      expectMsg(jobInfo)
    }

    it("should return job info when requested for jobId that exists, where the job is a Python job") {
      val dt = DateTime.parse("2013-05-29T00Z")
      val jobInfo = JobInfo(
        "bar", "cid", "context",
        "com.abc.meme", JobStatus.Running, dt, None, None, Seq(BinaryInfo("demo", BinaryType.Egg, dt)))
      dao.saveJobInfo(jobInfo)
      actor ! GetJobStatus("bar")
      expectMsg(jobInfo)
    }

    it("should return error if job info is requested for jobId that does not exist") {
      actor ! GetJobStatus("foo")
      expectMsg(NoSuchJobId)
    }

    it("should return list of job infos when requested for job statuses") {
      val dt1 = DateTime.parse("2013-05-28T00Z")
      val dt2 = DateTime.parse("2013-05-29T00Z")
      val jobInfo1 =
        JobInfo(
          "foo-1", "cid", "context",
          "com.abc.meme", JobStatus.Running, dt2, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt1))
        )
      val jobInfo2 =
        JobInfo(
          "foo-2", "cid", "context",
          "com.abc.meme", JobStatus.Running, dt2, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt2))
        )
      dao.saveJobInfo(jobInfo1)
      dao.saveJobInfo(jobInfo2)
      actor ! GetJobStatuses(Some(10))
      expectMsg(Seq[JobInfo](jobInfo1, jobInfo2))
    }

    it("should return as many number of job infos as requested") {
      val dt1 = DateTime.parse("2013-05-28T00Z")
      val dt2 = DateTime.parse("2013-05-29T00Z")
      val jobInfo1 =
        JobInfo("foo-1", "cid", "context", "com.abc.meme", JobStatus.Running, dt1, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt1)))
      val jobInfo2 =
        JobInfo("foo-2", "cid", "context", "com.abc.meme", JobStatus.Running, dt2, None, None, Seq(BinaryInfo("demo", BinaryType.Egg, dt2)))
      dao.saveJobInfo(jobInfo1)
      dao.saveJobInfo(jobInfo2)
      actor ! GetJobStatuses(Some(1))
      expectMsg(Seq[JobInfo](jobInfo1))
    }
    it("should return job infos as requested status ") {
      val dt1 = DateTime.parse("2013-05-28T00Z")
      val dt2 = dt1.plusMinutes(5)
      val dt3 = dt2.plusMinutes(5)
      val dt4 = dt3.plusMinutes(5)
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, dt1)
      val someError = Some(ErrorData(new Throwable("test-error")))
      val runningJob = JobInfo("running-1", "cid", "context", "com.abc.meme",
        JobStatus.Running, dt1, None, None, Seq(binaryInfo))
      val errorJob = JobInfo("error-1", "cid", "context", "com.abc.meme",
        JobStatus.Error, dt2, Some(dt2), someError, Seq(binaryInfo))
      val finishedJob = JobInfo("finished-1", "cid", "context", "com.abc.meme",
        JobStatus.Finished, dt3, Some(dt4), None, Seq(binaryInfo))

      dao.saveJobInfo(runningJob)
      dao.saveJobInfo(errorJob)
      dao.saveJobInfo(finishedJob)

      actor ! GetJobStatuses(Some(1), Some(JobStatus.Running))
      expectMsg(Seq[JobInfo](runningJob))
      actor ! GetJobStatuses(Some(1), Some(JobStatus.Error))
      expectMsg(Seq[JobInfo](errorJob))
      actor ! GetJobStatuses(Some(1), Some(JobStatus.Finished))
      expectMsg(Seq[JobInfo](finishedJob))
      actor ! GetJobStatuses(Some(10), None)
      expectMsg(Seq[JobInfo](runningJob, errorJob, finishedJob))
      actor ! GetJobStatuses(Some(10))
      expectMsg(Seq[JobInfo](runningJob, errorJob, finishedJob))


    }
    it("should return empty list if jobs doest not exist") {
      actor ! GetJobStatuses(Some(1))
      expectMsg(Seq.empty)
    }

    it("should return error if job result is requested for jobId that does not exist") {
      actor ! GetJobResult("foo")
      expectMsg(NoSuchJobId)
    }

    it("should return job info if job result is requested for running or errored out or killed job") {
      val someError = Some(ErrorData(new Throwable("test-error")))
      val dt1 = DateTime.parse("2013-05-28T00Z")
      val dt2 = DateTime.parse("2013-05-29T00Z")
      val jobInfo1 =
        JobInfo("foo-1", "cid", "context", "com.abc.meme",
          JobStatus.Running, dt1, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt1)))
      val jobInfo2 =
        JobInfo("foo-2", "cid", "context", "com.abc.meme",
          JobStatus.Error, dt2, Some(DateTime.now()), someError, Seq(BinaryInfo("demo", BinaryType.Jar, dt2)))
      val jobInfo3 =
        JobInfo("foo-3", "cid", "context", "com.abc.meme", JobStatus.Killed,
          dt2, Some(DateTime.now()), someError, Seq(BinaryInfo("demo", BinaryType.Jar, dt2)))
      dao.saveJobInfo(jobInfo1)
      dao.saveJobInfo(jobInfo2)
      dao.saveJobInfo(jobInfo3)

      actor ! GetJobResult("foo-1")
      expectMsg(jobInfo1)

      actor ! GetJobResult("foo-2")
      expectMsg(jobInfo2)

      actor ! GetJobResult("foo-3")
      expectMsg(jobInfo3)
    }
  }
}
