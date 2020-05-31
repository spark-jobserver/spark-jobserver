package spark.jobserver.io

import java.io.File

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.JobManagerActor.ContextTerminatedException
import spark.jobserver.io.JobDAOActor._
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.util._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object JobDAOActorSpec {
  val system = ActorSystem("dao-test")
  val dt = DateTime.now()
  val dtplus1 = dt.plusHours(1)

  val cleanupProbe = TestProbe()(system)
  val unblockingProbe = TestProbe()(system)
  val spyProbe = TestProbe()(system)

  def config: Config = ConfigFactory.load("local.test.dao.conf")
}

class JobDAOActorSpec extends TestKit(JobDAOActorSpec.system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import JobDAOActorSpec._
  implicit val futureTimeout = Timeout(10.seconds)
  private val defaultTimeout = 10.seconds
  private val shortTimeout: FiniteDuration = 3 seconds
  val dummyMetaDataDao = new DummyMetaDataDAO(config)
  val dummyBinaryDao = new DummyBinaryDAO(config)
  val daoActor = system.actorOf(JobDAOActor.props(dummyMetaDataDao, dummyBinaryDao, config))
  var inMemoryDaoActor: ActorRef = _
  var inMemoryMetaDAO: MetaDataDAO = _
  var inMemoryBinDAO: BinaryDAO = _

  before {
    DAOTestsHelper.testProbe = TestProbe()(system)
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryDAO
    inMemoryDaoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, config))
  }

  override def afterAll() {
    AkkaTestUtils.shutdownAndWait(system)
  }

  describe("JobDAOActor with mocked Meta and Binary DAO") {

    it("should save binary and metadata for binary and respond with success") {
      daoActor ! SaveBinary("success", BinaryType.Jar, DateTime.now, DAOTestsHelper.binaryDAOBytesSuccess)
      DAOTestsHelper.testProbe.expectMsg("BinaryDAO: Save success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: Save success")
      expectMsg(SaveBinaryResult(Success({})))
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should respond when saving Binary fails") {
      daoActor ! SaveBinary("binarySaveFail", BinaryType.Jar, DateTime.now, DAOTestsHelper.binaryDAOBytesFail)
      DAOTestsHelper.testProbe.expectMsg("BinaryDAO: Save failed")
      expectMsgPF(3 seconds){
        case SaveBinaryResult(Failure(ex)) if ex.getMessage.startsWith("can't save binary") =>
      }
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should try to delete binary if meta data save failed") {
      daoActor ! SaveBinary("failed", BinaryType.Jar, DateTime.now, DAOTestsHelper.binaryDAOBytesSuccess)
      DAOTestsHelper.testProbe.expectMsg("BinaryDAO: Save success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: Save failed")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinariesByStorageId success")
      DAOTestsHelper.testProbe.expectMsg("BinaryDAO: Delete success")
      expectMsgPF(3 seconds){
        case SaveBinaryResult(Failure(ex)) if ex.getMessage.startsWith("can't save binary") =>
      }
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should not block other calls to DAO if save binary is taking too long") {
      daoActor ! SaveBinary("long-call-400", BinaryType.Jar,
        DateTime.now, DAOTestsHelper.binaryDAOBytesSuccess)

      daoActor ! GetJobInfos(0)
      expectMsg(1.seconds, JobInfos(Seq()))

      daoActor ! SaveBinary("success", BinaryType.Jar, DateTime.now, DAOTestsHelper.binaryDAOBytesSuccess)
      expectMsg(1.seconds, SaveBinaryResult(Success({})))

      daoActor ! DeleteBinary("failOnThis")
      expectMsgPF(1.seconds){
        case DeleteBinaryResult(Failure(ex)) if ex.getMessage.startsWith("can't find binary") =>
      }

      expectMsg(4.seconds, SaveBinaryResult(Success({})))
    }

    it("should delete binary correctly: delete binary and metadata and respond with success") {
      daoActor ! DeleteBinary("success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinary success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: Delete success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinariesByStorageId success")
      DAOTestsHelper.testProbe.expectMsg("BinaryDAO: Delete success")
      expectMsg(DeleteBinaryResult(Success({})))
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should not delete binary if meta is not deleted") {
      daoActor ! DeleteBinary("get-info-success-del-info-failed")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinary success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: Delete failed")
      expectMsgPF(3 seconds){
        case DeleteBinaryResult(Failure(ex: DeleteBinaryInfoFailedException)) =>
          ex.getMessage should startWith("can't delete meta data")
      }
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should not delete binary from binary storage if it is still used") {
      daoActor ! DeleteBinary(DAOTestsHelper.someBinaryName)
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinary success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: Delete success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinariesByStorageId success")
      expectMsg(DeleteBinaryResult(Success({})))
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should delete binary from binary storage if it is not in use") {
      daoActor ! DeleteBinary(DAOTestsHelper.someOtherBinaryName)
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinary success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: Delete success")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinariesByStorageId success")
      DAOTestsHelper.testProbe.expectMsg("BinaryDAO: Delete success")
      expectMsg(DeleteBinaryResult(Success({})))
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should throw NoSuchBinaryException if metadata info was not found") {
      daoActor ! DeleteBinary("get-info-failed")
      DAOTestsHelper.testProbe.expectMsg("MetaDataDAO: getBinary failed")
      expectMsgPF(3 seconds){
        case DeleteBinaryResult(Failure(ex: NoSuchBinaryException)) =>
          ex.getMessage should startWith("can't find binary")
      }
      DAOTestsHelper.testProbe.expectNoMsg(shortTimeout)
    }

    it("should respond when deleting Binary fails") {
      daoActor ! DeleteBinary("failOnThis")
      expectMsgPF(3 seconds){
        case DeleteBinaryResult(Failure(ex)) if ex.getMessage.startsWith("can't find binary") =>
      }
    }

    it("should return apps") {
      daoActor ! GetApps(None)
      expectMsg(Apps(Map(
        DAOTestsHelper.someBinaryName -> (BinaryType.Jar, DAOTestsHelper.defaultDate),
        DAOTestsHelper.someOtherBinaryName -> (BinaryType.Jar, DAOTestsHelper.defaultDate),
        "name3" -> (BinaryType.Jar, DAOTestsHelper.defaultDate),
        "name4" -> (BinaryType.Jar, DAOTestsHelper.defaultDate),
        "name5" -> (BinaryType.Jar, DAOTestsHelper.defaultDate)
      )))
    }

    it("should get JobInfos") {
      daoActor ! GetJobInfos(0)
      expectMsg(JobInfos(Seq()))
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
      failedMsg.error.getMessage should startWith("can't save context")
    }

    it("should update context by id with all attributes") {
      daoActor ! SaveContextInfo(ContextInfo("success", "name", "config", None,
        DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      val endTime = DateTime.now()
      daoActor ! UpdateContextById("success", ContextInfoModifiable(
          Some("new-address"), Some(endTime), ContextStatus.Error, Some(new Exception("Yay!"))))
      expectMsg(SavedSuccessfully)

      daoActor ! GetContextInfo("success")
      val msg = expectMsgType[ContextResponse]
      val context = msg.contextInfo.get
      context.state should be(ContextStatus.Error)
      context.actorAddress.get should be("new-address")
      context.endTime.get should be(endTime)
      context.error.get.getMessage should be("Yay!")
    }

    it("should update with new values and if final state is being set then should also set the end time") {
      val contextId = "update-with-address"
      daoActor ! SaveContextInfo(ContextInfo(contextId, "name", "config", Some("address"),
        DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Killed, Some(new Exception("Nooo!"))))
      expectMsg(SavedSuccessfully)

      daoActor ! GetContextInfo(contextId)
      val msg = expectMsgType[ContextResponse]
      val context = msg.contextInfo.get
      context.id should be(contextId)
      context.state should be(ContextStatus.Killed)
      context.actorAddress.get should be("address")
      context.endTime should not be(None)
      context.error.get.getMessage should be("Nooo!")
    }

    it("should update with new values and if non-final state is being set then endTime should be None") {
      val contextId = "update-non-final"
      daoActor ! SaveContextInfo(ContextInfo(contextId, "name", "config", None,
        DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)

      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Stopping))
      expectMsg(SavedSuccessfully)

      daoActor ! GetContextInfo(contextId)
      val msg = expectMsgType[ContextResponse].contextInfo.get
      msg.id should be(contextId)
      msg.state should be(ContextStatus.Stopping)
      msg.actorAddress should be(None)
      msg.endTime should be(None)
      msg.error should be(None)
    }

    it("should respond with SaveFailed if DAO calls fails (no context found)") {
      val contextId2 = "update-not-found"
      daoActor ! UpdateContextById(contextId2, ContextInfoModifiable(
        ContextStatus.Running))
      expectMsgType[SaveFailed].error.getMessage() should be(NoMatchingDAOObjectException().getMessage)
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
      val cp = Seq("success", "hdfs://uri2/uri")
      daoActor ! GetBinaryInfosForCp(cp)

      val response = expectMsgType[BinaryInfosForCp].binInfos
      response.map(_.appName) should be (cp)
      response.map(_.binaryType) should be (Seq(BinaryType.Jar, BinaryType.URI))
    }

    it("should return GetBinaryInfosForCpFailed if during URI parsing some exception occurs") {
      val cp = Seq("success", "://uri2:/uri")
      daoActor ! GetBinaryInfosForCp(cp)

      val response = expectMsgType[GetBinaryInfosForCpFailed]
      response.error.getMessage.startsWith("java.net.URISyntaxException: Expected scheme name at index")
    }

    it("should reply with JobConfigStoreFailed if saving job config failed") {
      val jobId = "job-config-fail"
      val config = ConfigFactory.parseString("{bugatti=justOk}")
      daoActor ! SaveJobConfig(jobId, config)
      expectMsg(2.second, JobConfigStoreFailed)
    }

    it("should return failure if job info save was unsuccessful") {
      val jobInfo = JobInfo("jid-fail", "", "", "", "", DAOTestsHelper.defaultDate,
        None, None, Seq(BinaryInfo("", BinaryType.Jar, DAOTestsHelper.defaultDate)))

      daoActor ! SaveJobInfo(jobInfo)
      expectMsg(false)
    }
  }

  describe("CleanContextJobInfos tests using InMemoryDAO") {
    it("should set jobs to error state if running") {
      val date = DateTime.now()
      val contextId = "ctxId"
      val jobId = "dummy"
      val endTime = DateTime.now()
      val terminatedException = Some(ErrorData(ContextTerminatedException(contextId)))
      val runningJob = JobInfo(jobId, contextId, "",
        "", JobStatus.Running, date, None, None, Seq(BinaryInfo("", BinaryType.Jar, date)))

      Await.result(inMemoryDaoActor ? SaveJobInfo(runningJob), shortTimeout)
      inMemoryDaoActor ! CleanContextJobInfos(contextId, endTime)

      Utils.retry(10) {
        val fetchedJob = Await.result(inMemoryDaoActor ? GetJobInfo(jobId), shortTimeout).
          asInstanceOf[Option[JobInfo]].get
        fetchedJob.contextId should be(contextId)
        fetchedJob.jobId should be(jobId)
        fetchedJob.state should be(JobStatus.Error)
        fetchedJob.endTime.get should be(endTime)
        fetchedJob.error.get.message should be(terminatedException.get.message)
      }
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
      val saveJob1Future = inMemoryDaoActor ? SaveJobInfo(jobInfo1)
      Await.result(saveJob1Future, defaultTimeout)
      val saveJob2Future = inMemoryDaoActor ? SaveJobInfo(jobInfo2)
      Await.result(saveJob2Future, defaultTimeout)
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
      val saveJob1Future = inMemoryDaoActor ? SaveJobInfo(jobInfo1)
      Await.result(saveJob1Future, defaultTimeout)
      val saveJob2Future = inMemoryDaoActor ? SaveJobInfo(jobInfo2)
      Await.result(saveJob2Future, defaultTimeout)
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

      val saveJob1Future = inMemoryDaoActor ? SaveJobInfo(runningJob)
      Await.result(saveJob1Future, defaultTimeout)
      val saveJob2Future = inMemoryDaoActor ? SaveJobInfo(errorJob)
      Await.result(saveJob2Future, defaultTimeout)
      val saveJob3Future = inMemoryDaoActor ? SaveJobInfo(finishedJob)
      Await.result(saveJob3Future, defaultTimeout)

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

  describe("SaveJobConfig/GetJobConfig tests using InMemoryDAO") {
    val jobId = "jobId"
    val jobConfig = ConfigFactory.empty()

    it("should store a job configuration") {
      inMemoryDaoActor ! SaveJobConfig(jobId, jobConfig)
      expectMsg(2.second, JobConfigStored)
      val storedJobConfig = Await.result(inMemoryMetaDAO.getJobConfig(jobId), 10 seconds)
      storedJobConfig should be (Some(jobConfig))
    }

    it("should return a job configuration when the jobId exists") {
      inMemoryDaoActor ! SaveJobConfig(jobId, jobConfig)
      expectMsg(2.seconds, JobConfigStored)
      inMemoryDaoActor ! GetJobConfig(jobId)
      expectMsg(JobConfig(Some((jobConfig))))
    }

    it("should return error if jobId does not exist") {
      inMemoryDaoActor ! GetJobConfig(jobId)
      expectMsg(JobConfig(None))
    }
  }

  describe("GetJobInfo tests using InMemoryDAO") {
    it("should return job info when requested for jobId that exists") {
      val dt = DateTime.parse("2013-05-29T00Z")
      val jobInfo = JobInfo("foo", "cid", "context", "com.abc.meme",
        JobStatus.Running, dt, None, None, Seq(BinaryInfo("demo", BinaryType.Jar, dt)))
      val saveJobFuture = inMemoryDaoActor ? SaveJobInfo(jobInfo)
      Await.result(saveJobFuture, defaultTimeout)

      inMemoryDaoActor ! GetJobInfo("foo")

      expectMsg(Some(jobInfo))
    }

    it("should return job info when requested for jobId that exists, where the job is a Python job") {
      val dt = DateTime.parse("2013-05-29T00Z")
      val jobInfo = JobInfo(
        "bar", "cid", "context",
        "com.abc.meme", JobStatus.Running, dt, None, None, Seq(BinaryInfo("demo", BinaryType.Egg, dt)))
      val saveJobFuture = inMemoryDaoActor ? SaveJobInfo(jobInfo)
      Await.result(saveJobFuture, defaultTimeout)

      inMemoryDaoActor ! GetJobInfo("bar")

      expectMsg(Some(jobInfo))
    }

    it("should return error if job info is requested for jobId that does not exist") {
      inMemoryDaoActor ! GetJobInfo("foo")
      expectMsg(None)
    }
  }

  describe("Cache-on-upload tests") {

    def saveBinaryAndCheckResponse(jobDAOActor: ActorRef, binName: String): Unit = {
      Await.result(jobDAOActor ? SaveBinary(binName,
        BinaryType.Jar,
        DAOTestsHelper.defaultDate,
        DAOTestsHelper.binaryDAOBytesSuccess), defaultTimeout)
      val binOption = Await.result(jobDAOActor ? GetLastBinaryInfo(binName), defaultTimeout).
        asInstanceOf[LastBinaryInfo].lastBinaryInfo
      val bin = binOption.get
      bin.appName should be(binName)
      bin.binaryType should be(BinaryType.Jar)
      bin.uploadTime should be(DAOTestsHelper.defaultDate)
    }

    def deleteBinaryAndCheckResponse(jobDAOActor: ActorRef, binName: String): Unit = {
      Await.result(jobDAOActor ? DeleteBinary(binName), defaultTimeout)
      val binInfo = Await.result(jobDAOActor ? GetLastBinaryInfo(binName), defaultTimeout).
        asInstanceOf[LastBinaryInfo].lastBinaryInfo
      binInfo should be(None)
    }

    it("should create cache on save binary and delete on delete binary if enabled") {
      val enabledCachingConfig = ConfigFactory.parseString("spark.jobserver.cache-on-upload = true").
        withFallback(config)
      val daoActorWithEnabledCaching = system.actorOf(JobDAOActor.props(
        new InMemoryMetaDAO, new InMemoryBinaryDAO, enabledCachingConfig))
      val binName = "success"
      val jarFile = new File(config.getString("spark.jobserver.combineddao.rootdir"),
        binName + "-" + DAOTestsHelper.defaultDate.toString("yyyyMMdd_HHmmss_SSS") + ".jar")

      jarFile.exists() should be(false)

      saveBinaryAndCheckResponse(daoActorWithEnabledCaching, binName)
      jarFile.exists() should be(true)

      deleteBinaryAndCheckResponse(daoActorWithEnabledCaching, binName)
      jarFile.exists() should be(false)
    }

    it("should not cache any binary if disabled") {
      val disabledCachingConfig = ConfigFactory.parseString("spark.jobserver.cache-on-upload = false").
        withFallback(config)
      val daoActorWithoutCache = system.actorOf(JobDAOActor.props(
        new InMemoryMetaDAO, new InMemoryBinaryDAO, disabledCachingConfig))
      val binName = "success"
      val jarFile = new File(config.getString("spark.jobserver.combineddao.rootdir"),
        binName + "-" + DAOTestsHelper.defaultDate.toString("yyyyMMdd_HHmmss_SSS") + ".jar")

      saveBinaryAndCheckResponse(daoActorWithoutCache, binName)
      jarFile.exists() should be(false)
    }
  }
}
