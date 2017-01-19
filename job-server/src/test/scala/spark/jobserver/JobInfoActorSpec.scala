package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
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

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    val supervisor = system.actorOf(Props(classOf[LocalContextSupervisorActor], daoActor))
    actor = system.actorOf(Props(classOf[JobInfoActor], dao, supervisor))
  }

  after {
    AkkaTestUtils.shutdownAndWait(actor)
  }

  describe("JobInfoActor") {
    it("should store a job configuration") {
      actor ! StoreJobConfig(jobId, jobConfig)
      expectMsg(JobConfigStored)
      val jobConfigs = Await.result(dao.getJobConfigs, 60 seconds)
      jobConfigs.get(jobId) should be (Some(jobConfig))
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
      val jobInfo =
        JobInfo("foo", "context", BinaryInfo("demo", BinaryType.Jar, dt), "com.abc.meme", dt, None, None)
      dao.saveJobInfo(jobInfo)
      actor ! GetJobStatus("foo")
      expectMsg(jobInfo)
    }

    it("should return job info when requested for jobId that exists, where the job is a Python job") {
      val dt = DateTime.parse("2013-05-29T00Z")
      val jobInfo =
        JobInfo("bar", "context", BinaryInfo("demo", BinaryType.Egg, dt), "com.abc.meme", dt, None, None)
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
        JobInfo("foo-1", "context", BinaryInfo("demo", BinaryType.Jar, dt1), "com.abc.meme", dt2, None, None)
      val jobInfo2 =
        JobInfo("foo-2", "context", BinaryInfo("demo", BinaryType.Jar, dt2), "com.abc.meme", dt2, None, None)
      dao.saveJobInfo(jobInfo1)
      dao.saveJobInfo(jobInfo2)
      actor ! GetJobStatuses(Some(10))
      expectMsg(Seq[JobInfo](jobInfo1, jobInfo2))
    }

    it("should return as many number of job infos as requested") {
      val dt1 = DateTime.parse("2013-05-28T00Z")
      val dt2 = DateTime.parse("2013-05-29T00Z")
      val jobInfo1 =
        JobInfo("foo-1", "context", BinaryInfo("demo", BinaryType.Jar, dt1), "com.abc.meme", dt1, None, None)
      val jobInfo2 =
        JobInfo("foo-2", "context", BinaryInfo("demo", BinaryType.Egg, dt2), "com.abc.meme", dt2, None, None)
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
      val someError =  Some(new Throwable("test-error"))
      val runningJob = JobInfo("running-1", "context", binaryInfo, "com.abc.meme", dt1, None, None)
      val errorJob = JobInfo("error-1", "context", binaryInfo, "com.abc.meme", dt2, None, someError)
      val finishedJob = JobInfo("finished-1", "context", binaryInfo, "com.abc.meme", dt3, Some(dt4), None)

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

    it("should return job info if job result is requested for running or errored out job") {
      val someError: Option[Throwable] = Some(new Throwable("test-error"))
      val dt1 = DateTime.parse("2013-05-28T00Z")
      val dt2 = DateTime.parse("2013-05-29T00Z")
      val jobInfo1 =
        JobInfo("foo-1", "context", BinaryInfo("demo", BinaryType.Jar, dt1), "com.abc.meme", dt1, None, None)
      val jobInfo2 =
        JobInfo("foo-2", "context", BinaryInfo("demo", BinaryType.Jar, dt2),
          "com.abc.meme", dt2, None, someError)
      dao.saveJobInfo(jobInfo1)
      dao.saveJobInfo(jobInfo2)
      actor ! GetJobResult("foo-1")
      expectMsg(jobInfo1)
      actor ! GetJobResult("foo-2")
      expectMsg(jobInfo2)
    }
  }
}
