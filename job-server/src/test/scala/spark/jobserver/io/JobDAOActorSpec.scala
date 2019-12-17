package spark.jobserver.io

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.InMemoryDAO
import spark.jobserver.io.JobDAOActor._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.util.NoMatchingDAOObjectException

object JobDAOActorSpec {
  val system = ActorSystem("dao-test")
  val dt = DateTime.now()
  val dtplus1 = dt.plusHours(1)

  val cleanupProbe = TestProbe()(system)
  val unblockingProbe = TestProbe()(system)
  val spyProbe = TestProbe()(system)

  object DummyDao extends JobDAO{
    override def saveBinary(appName: String, binaryType: BinaryType,
                            uploadTime: DateTime, binaryBytes: Array[Byte]): Unit = {
      appName match {
        case "failOnThis" => throw new Exception("deliberate failure")
        case "blockDAO" => unblockingProbe.expectMsg(5.seconds, "unblock")
        case _ => //Do nothing
      }
    }

    override def getApps: Future[Map[String, (BinaryType, DateTime)]] =
      Future.successful(Map(
        "app1" -> (BinaryType.Jar, dt),
        "app2" -> (BinaryType.Egg, dtplus1)
      ))

    override def getBinaryFilePath(appName: String,
                                   binaryType: BinaryType, uploadTime: DateTime): String = ???

    override def saveContextInfo(contextInfo: ContextInfo): Unit = {
      contextInfo.id match {
        case "success" =>
        case "update-running" | "update-with-address" | "update-non-final" =>
          spyProbe.ref ! contextInfo
          unblockingProbe.expectMsg("unblock")
        case "failure" | "update-fail" => throw new Exception("deliberate failure")
      }
    }

    override def getContextInfo(id: String): Future[Option[ContextInfo]] = {
      id match {
        case "update-running" => Future.successful(
          Option(ContextInfo("update-running", "name", "config", None,
            DateTime.now(), None, ContextStatus.Running, None)))
        case "update-with-address" => Future.successful(
          Option(ContextInfo("update-with-address", "name", "config", Some("address"),
            DateTime.now(), None, ContextStatus.Running, None)))
        case "update-non-final" => Future.successful(
          Option(ContextInfo("update-non-final", "name", "config", None,
            DateTime.now(), None, ContextStatus.Stopping, None)))
        case "update-dao-fail" => Future.failed(new Exception("deliberate failure"))
        case "update-not-found" => Future.successful(None)
        case "update-fail" => Future.successful(
          Option(ContextInfo("update-fail", "name", "config", None,
            DateTime.now(), None, ContextStatus.Running, None)))
        case _ => Future.successful(None)
      }
    }

    override def getContextInfos(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
      Future[Seq[ContextInfo]] = ???

    override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = ???

    override def saveJobConfig(jobId: String, jobConfig: Config): Unit = ???

    override def getJobInfos(limit: Int, status: Option[String]): Future[Seq[JobInfo]] =
      Future.successful(Seq())

    override def getJobInfosByContextId(
        contextId: String, jobStatuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = ???

    override def getJobInfo(jobId: String): Future[Option[JobInfo]] = ???

    override def saveJobInfo(jobInfo: JobInfo): Unit = ???

    override def getJobConfig(jobId: String): Future[Option[Config]] = ???

    override def getBinaryInfo(appName: String): Option[BinaryInfo] = appName match {
      case "foo" => Some(BinaryInfo("foo", BinaryType.Jar, DateTime.now()))
      case _ => None
    }

    override def deleteBinary(appName: String): Unit = {
      appName match {
        case "failOnThis" => throw new Exception("deliberate failure")
        case _ => //Do nothing
      }
    }

    override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
        Future[Seq[JobInfo]] = {
      binName match {
        case "empty" => Future.successful(Seq.empty)
        case "multiple" =>
          val dt = DateTime.parse("2013-05-29T00Z")
          val jobInfo =
            JobInfo("bar", "cid", "context",
              "com.abc.meme", JobStatus.Running, dt, None, None, Seq(BinaryInfo("demo", BinaryType.Egg, dt)))

          Future.successful(Seq(jobInfo, jobInfo.copy(jobId = "kaboom")))
        case _ => Future.failed(new Exception("Unknown message"))
      }
    }

    override def cleanRunningJobInfosForContext(contextName: String, endTime: DateTime): Future[Unit] = {
      cleanupProbe.ref ! contextName
      Future.successful(())
    }
  }
}

class JobDAOActorSpec extends TestKit(JobDAOActorSpec.system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import JobDAOActorSpec._

  val daoActor = system.actorOf(JobDAOActor.props(DummyDao))
  var inMemoryDaoActor: ActorRef = _
  var inMemoryDao: JobDAO = _

  before {
    inMemoryDao = new InMemoryDAO
    inMemoryDaoActor = system.actorOf(JobDAOActor.props(inMemoryDao))
  }

  override def afterAll() {
    AkkaTestUtils.shutdownAndWait(system)
  }

  describe("JobDAOActor") {

    it("should respond when saving Binary completes successfully") {
      daoActor ! SaveBinary("succeed", BinaryType.Jar, DateTime.now, Array[Byte]())
      expectMsg(SaveBinaryResult(Success({})))
    }

    it("should respond when saving Binary fails") {
      daoActor ! SaveBinary("failOnThis", BinaryType.Jar, DateTime.now, Array[Byte]())
      expectMsgPF(3 seconds){
        case SaveBinaryResult(Failure(ex)) if ex.getMessage == "deliberate failure" =>
      }
    }

    it("should not block other calls to DAO if save binary is taking too long") {
      daoActor ! SaveBinary("blockDAO", BinaryType.Jar, DateTime.now, Array[Byte]())

      daoActor ! GetJobInfos(1)
      expectMsg(1.seconds, JobInfos(Seq()))

      daoActor ! SaveBinary("succeed", BinaryType.Jar, DateTime.now, Array[Byte]())
      expectMsg(1.seconds, SaveBinaryResult(Success({})))

      daoActor ! DeleteBinary("failOnThis")
      expectMsgPF(1.seconds){
        case DeleteBinaryResult(Failure(ex)) if ex.getMessage == "deliberate failure" =>
      }

      unblockingProbe.ref ! "unblock"
      expectMsg(4.seconds, SaveBinaryResult(Success({})))
    }

    it("should respond when deleting Binary completes successfully") {
      daoActor ! DeleteBinary("succeed")
      expectMsg(DeleteBinaryResult(Success({})))
    }

    it("should respond when deleting Binary fails") {
      daoActor ! DeleteBinary("failOnThis")
      expectMsgPF(3 seconds){
        case DeleteBinaryResult(Failure(ex)) if ex.getMessage == "deliberate failure" =>
      }
    }

    it("should return apps") {
      daoActor ! GetApps(None)
      expectMsg(Apps(Map(
        "app1" -> (BinaryType.Jar, dt),
        "app2" -> (BinaryType.Egg, dtplus1)
      )))
    }

    it("should get JobInfos") {
      daoActor ! GetJobInfos(1)
      expectMsg(JobInfos(Seq()))
    }

    it("should request jobs cleanup") {
      daoActor ! CleanContextJobInfos("context", DateTime.now())
      cleanupProbe.expectMsg("context")
    }

    it("should respond with successful message if dao operation was successful") {
      daoActor ! SaveContextInfo(ContextInfo("success", "name", "config", None,
        DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)
    }

    it("should respond with failure message if dao operation has an exception") {
      daoActor ! SaveContextInfo(ContextInfo("failure", "name", "config", None,
        DateTime.now(), None, ContextStatus.Running, None))
      val failedMsg = expectMsgType[SaveFailed]
      failedMsg.error.getMessage should be("deliberate failure")
    }

    it("should update context by id with all attributes") {
      val contextId = "update-running"
      val endTime = DateTime.now()
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
          Some("new-address"), Some(endTime), ContextStatus.Error, Some(new Exception("Yay!"))))

      val msg = spyProbe.expectMsgType[ContextInfo]
      msg.id should be(contextId)
      msg.state should be(ContextStatus.Error)
      msg.actorAddress.get should be("new-address")
      msg.endTime.get should be(endTime)
      msg.error.get.getMessage should be("Yay!")
      unblockingProbe.ref ! "unblock"
      expectMsg(5.seconds, SavedSuccessfully)
    }

    it("should update with new values and if final state is being set then should also set the end time") {
      val contextId = "update-with-address"
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Killed, Some(new Exception("Nooo!"))))

      val msg = spyProbe.expectMsgType[ContextInfo]
      msg.id should be(contextId)
      msg.state should be(ContextStatus.Killed)
      msg.actorAddress.get should be("address")
      msg.endTime should not be(None)
      msg.error.get.getMessage should be("Nooo!")
      unblockingProbe.ref ! "unblock"
      expectMsg(5.seconds, SavedSuccessfully)
    }

    it("should update with new values and if non-final state is being set then endTime should be None") {
      val contextId = "update-non-final"
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Stopping))

      val msg = spyProbe.expectMsgType[ContextInfo]
      msg.id should be(contextId)
      msg.state should be(ContextStatus.Stopping)
      msg.actorAddress should be(None)
      msg.endTime should be(None)
      msg.error should be(None)
      unblockingProbe.ref ! "unblock"
      expectMsg(5.seconds, SavedSuccessfully)
    }

    it("should respond with SaveFailed if DAO calls fails (no context or exceptio)") {
      val contextId = "update-dao-fail"
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Restarting))
      expectMsgType[SaveFailed].error.getMessage() should be("deliberate failure")

      val contextId2 = "update-not-found"
      daoActor ! UpdateContextById(contextId2, ContextInfoModifiable(
        ContextStatus.Running))
      expectMsgType[SaveFailed].error.getMessage() should be(NoMatchingDAOObjectException().getMessage)

      val contextId3 = "update-fail"
      daoActor ! UpdateContextById(contextId3, ContextInfoModifiable(
        ContextStatus.Started))
      expectMsgType[SaveFailed].error.getMessage() should be("deliberate failure")
    }

    it("should get empty list of jobs if binary name is invalid") {
      daoActor ! GetJobsByBinaryName("empty")
      expectMsg(JobInfos(Seq()))
    }

    it("should get jobs by binary name") {
      daoActor ! GetJobsByBinaryName("multiple")

      expectMsgPF(5.seconds, "Get the jobs") {
        case JobInfos(jobs) => jobs.map(_.jobId) should contain allOf("bar", "kaboom")
      }
    }

    it("should return list of URI BinaryInfo objects for cp") {
      val cp = Seq("hdfs://uri2/uri", "http://uri.com")
      daoActor ! GetBinaryInfosForCp(cp)

      val response = expectMsgType[BinaryInfosForCp].binInfos
      response.map(_.appName) should be (cp)
      response.map(_.binaryType) should be (Seq(BinaryType.URI, BinaryType.URI))
    }

    it("should return BinaryNotFound if one of the binary is not found") {
      val cp = Seq("hdfs://uri2/uri", "noBinary", "foo")
      daoActor ! GetBinaryInfosForCp(cp)
      expectMsg(BinaryNotFound("noBinary"))
    }

    it("should return list of BinaryInfo objects from DB and for URIs") {
      val cp = Seq("foo", "hdfs://uri2/uri")
      daoActor ! GetBinaryInfosForCp(cp)

      val response = expectMsgType[BinaryInfosForCp].binInfos
      response.map(_.appName) should be (cp)
      response.map(_.binaryType) should be (Seq(BinaryType.Jar, BinaryType.URI))
    }

    it("should return GetBinaryInfosForCpFailed if during URI parsing some exception occurs") {
      val cp = Seq("foo", "://uri2:/uri")
      daoActor ! GetBinaryInfosForCp(cp)

      val response = expectMsgType[GetBinaryInfosForCpFailed]
      response.error.getMessage.startsWith("java.net.URISyntaxException: Expected scheme name at index")
    }
  }

  describe("GetJobInfos tests using InMemoryDAO") {
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
      inMemoryDao.saveJobInfo(jobInfo1)
      inMemoryDao.saveJobInfo(jobInfo2)
      inMemoryDaoActor ! GetJobInfos(10)
      expectMsg(JobInfos(Seq[JobInfo](jobInfo1, jobInfo2)))
    }

    it("should return as many number of job infos as requested") {
      val dt1 = DateTime.parse("2013-05-28T00Z")
      val dt2 = DateTime.parse("2013-05-29T00Z")
      val jobInfo1 = JobInfo("foo-1", "cid", "context", "com.abc.meme",
        JobStatus.Running, dt1, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt1)))
      val jobInfo2 = JobInfo("foo-2", "cid", "context", "com.abc.meme",
        JobStatus.Running, dt2, None, None, Seq(BinaryInfo("demo", BinaryType.Egg, dt2)))
      inMemoryDao.saveJobInfo(jobInfo1)
      inMemoryDao.saveJobInfo(jobInfo2)
      inMemoryDaoActor ! GetJobInfos(1)
      expectMsg(JobInfos(Seq[JobInfo](jobInfo1)))
    }

    it("should return job infos as requested status") {
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

      inMemoryDao.saveJobInfo(runningJob)
      inMemoryDao.saveJobInfo(errorJob)
      inMemoryDao.saveJobInfo(finishedJob)

      inMemoryDaoActor ! GetJobInfos(1, Some(JobStatus.Running))
      expectMsg(JobInfos(Seq[JobInfo](runningJob)))
      inMemoryDaoActor ! GetJobInfos(1, Some(JobStatus.Error))
      expectMsg(JobInfos(Seq[JobInfo](errorJob)))
      inMemoryDaoActor ! GetJobInfos(1, Some(JobStatus.Finished))
      expectMsg(JobInfos(Seq[JobInfo](finishedJob)))
      inMemoryDaoActor ! GetJobInfos(10, None)
      expectMsg(JobInfos(Seq[JobInfo](runningJob, errorJob, finishedJob)))
      inMemoryDaoActor ! GetJobInfos(10)
      expectMsg(JobInfos(Seq[JobInfo](runningJob, errorJob, finishedJob)))
    }

    it("should return empty list if jobs doest not exist") {
      inMemoryDaoActor ! GetJobInfos(1)
      expectMsg(JobInfos(Seq.empty))
    }
  }

  describe("SaveJobConfig tests using InMemoryDAO") {
    val jobId = "jobId"
    val jobConfig = ConfigFactory.empty()

    it("should store a job configuration") {
      inMemoryDaoActor ! SaveJobConfig(jobId, jobConfig)
      expectMsg(2.second, JobConfigStored)
      val storedJobConfig = Await.result(inMemoryDao.getJobConfig(jobId), 10 seconds)
      storedJobConfig should be (Some(jobConfig))
    }

    it("should return a job configuration when the jobId exists") {
      inMemoryDaoActor ! SaveJobConfig(jobId, jobConfig)
      expectMsg(2.seconds, JobConfigStored)
      inMemoryDaoActor ! GetJobConfig(jobId)
      expectMsg(JobConfig(Some((jobConfig))))
    }
  }
}
