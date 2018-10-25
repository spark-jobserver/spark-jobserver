package spark.jobserver.io

import scala.concurrent.Await
import scala.concurrent.duration._

import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.TestJarFinder
import java.util.UUID
import org.slf4j.LoggerFactory

abstract class SqlCommonSpecBase {
  def config : Config
}

class SqlCommonSpec extends SqlCommonSpecBase with TestJarFinder with FunSpecLike with Matchers
  with BeforeAndAfter  with BeforeAndAfterAll{
  override def config: Config = ConfigFactory.load("local.test.jobsqldao.conf")
  private val logger = LoggerFactory.getLogger(getClass)
  val timeout = 60 seconds
  var dao: SqlCommon = _
  var helperJobSqlDao: JobSqlDAO = _

  val time: DateTime = new DateTime()
  val throwable: Throwable = new Throwable("test-error")

  // jar test data
  val jarInfo: BinaryInfo = genJarInfo(false, false)
  val jarBytes: Array[Byte] = Files.toByteArray(testJar)
  val eggInfo: BinaryInfo = BinaryInfo("myEggBinary", BinaryType.Egg, time)

  // jobInfo test data
  val jobInfoNoEndNoErr:JobInfo = genJobInfo(jarInfo, false, JobStatus.Running)
  val expectedJobInfo = jobInfoNoEndNoErr
  val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, false, JobStatus.Finished)
  val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, false, ContextStatus.Error)

  // job config test data
  val jobId: String = jobInfoNoEndNoErr.jobId
  val jobConfig: Config = ConfigFactory.parseString("{marco=pollo}")
  val expectedConfig: Config =
    ConfigFactory.empty().withValue("marco", ConfigValueFactory.fromAnyRef("pollo"))

  // Helper functions and closures
  private def genJarInfoClosure = {
    var appCount: Int = 0
    var timeCount: Int = 0

    def genTestJarInfo(newAppName: Boolean, newTime: Boolean): BinaryInfo = {
      appCount = appCount + (if (newAppName) 1 else 0)
      timeCount = timeCount + (if (newTime) 1 else 0)

      val app = "test-appName" + appCount
      val upload = if (newTime) time.plusMinutes(timeCount) else time

      BinaryInfo(app, BinaryType.Jar, upload)
    }

    genTestJarInfo _
  }

  case class GenJobInfoClosure() {
    var count: Int = 0

    def apply(jarInfo: BinaryInfo, isNew:Boolean, state: String,
        contextId: Option[String] = None): JobInfo = {
      count = count + (if (isNew) 1 else 0)
      val id: String = "test-id" + count

      val ctxId = contextId match {
        case Some(id) => id
        case None => "test-context-id" + count
      }

      val contextName: String = "test-context"
      val classPath: String = "test-classpath"
      val startTime: DateTime = time

      val noEndTime: Option[DateTime] = None
      val someEndTime: Option[DateTime] = Some(time)
      val someError = Some(ErrorData(throwable))

      val endTimeAndError = state match {
        case JobStatus.Started | JobStatus.Running => (None, None)
        case JobStatus.Finished => (someEndTime, None)
        case JobStatus.Error | JobStatus.Killed => (someEndTime, someError)
      }

      JobInfo(id, ctxId, contextName, jarInfo, classPath, state, startTime,
          endTimeAndError._1, endTimeAndError._2)
    }
  }

  def genJarInfo: (Boolean, Boolean) => BinaryInfo = genJarInfoClosure
  lazy val genJobInfo = GenJobInfoClosure()

  override def beforeAll() {
    helperJobSqlDao = new JobSqlDAO(config)
    dao = new SqlCommon(config)
    helperJobSqlDao.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)
  }

  override def afterAll() {
    helperJobSqlDao.deleteBinary(jarInfo.appName)
  }

  describe("saveJobConfig() tests") {
    it("should provide None on getJobConfig(jobId) where there is no config for a given jobId") {
      val config = Await.result(dao.getJobConfig("44c32fe1-38a4-11e1-a06a-485d60c81a3e"), timeout)
      config shouldBe None
    }

    it("should save and get the same config") {
      dao.saveJobConfig(jobId, jobConfig)

      val config = Await.result(dao.getJobConfig(jobId), timeout).get
      config should equal (expectedConfig)
    }

    it("Save a new config, bring down DB, bring up DB, should get configs from DB") {
      val jobId2: String = genJobInfo(genJarInfo(false, false), true, JobStatus.Running).jobId
      val jobConfig2: Config = ConfigFactory.parseString("{merry=xmas}")
      val expectedConfig2 = ConfigFactory.empty().withValue("merry", ConfigValueFactory.fromAnyRef("xmas"))
      dao.saveJobConfig(jobId, jobConfig)
      dao.saveJobConfig(jobId2, jobConfig2)

      // Destroy and bring up the DB again
      helperJobSqlDao = null
      helperJobSqlDao = new JobSqlDAO(config)

      val jobIdConfig = Await.result(dao.getJobConfig(jobId), timeout).get
      val jobId2Config = Await.result(dao.getJobConfig(jobId2), timeout).get

      jobIdConfig should equal (expectedConfig)
      jobId2Config should equal (expectedConfig2)
    }
  }

  describe("Basic saveJobInfo() and getJobInfos() tests") {
    it("should provide an empty Seq on getJobInfos() for an empty JOBS table") {
      Seq.empty[JobInfo] should equal (Await.result(dao.getJobs(1), timeout))
    }

    it("should save a new JobInfo and get the same JobInfo") {
      helperJobSqlDao.saveJobInfo(jobInfoNoEndNoErr)

      val jobs: Seq[JobInfo] = Await.result(dao.getJobs(10), timeout)

      jobs.head.jobId should equal (jobId)
      jobs.head should equal (expectedJobInfo)
    }

    it("retrieve by status") {
      val dt1 = DateTime.now()
      val dt2 = Some(DateTime.now())
      val someError = Some(ErrorData("test-error", "", ""))
      val finishedJob: JobInfo =
        JobInfo("test-finished", "cid","test", jarInfo, "test-class", JobStatus.Finished, dt1, dt2, None)
      val errorJob: JobInfo =
        JobInfo("test-error", "cid", "test", jarInfo, "test-class", JobStatus.Error, dt1, dt2, someError)
      val runningJob: JobInfo =
        JobInfo("test-running", "cid", "test", jarInfo, "test-class", JobStatus.Running, dt1, None, None)
      helperJobSqlDao.saveJobInfo(finishedJob)
      helperJobSqlDao.saveJobInfo(runningJob)
      helperJobSqlDao.saveJobInfo(errorJob)

      val retrievedRunning = Await.result(dao.getJobs(1, Some(JobStatus.Running)), timeout).head
      retrievedRunning.endTime.isDefined should equal (false)
      retrievedRunning.error.isDefined should equal (false)

      val retrievedFinished = Await.result(dao.getJobs(1, Some(JobStatus.Finished)), timeout).head
      retrievedFinished.endTime.isDefined should equal (true)
      retrievedFinished.error.isDefined should equal (false)

      val retrievedError = Await.result(dao.getJobs(1, Some(JobStatus.Error)), timeout).head
      retrievedError.error.isDefined should equal (true)
    }

    it("should retrieve jobs by context id") {
      val contextId = UUID.randomUUID().toString()
      val runningJob = genJobInfo(jarInfo, true, JobStatus.Running, Some(contextId))
      helperJobSqlDao.saveJobInfo(runningJob)

      val retrieved = Await.result(dao.getJobsByContextId(contextId), timeout)
      retrieved.size shouldBe 1
      retrieved.head.contextId shouldBe contextId
    }

    it("should retrieve multiple jobs if they have the same context id") {
      val contextId = UUID.randomUUID().toString()
      val finishedJob = genJobInfo(jarInfo, true, JobStatus.Finished, Some(contextId))
      val jobWithDifferentContextId = genJobInfo(jarInfo, true, JobStatus.Finished)
      val runningJob = genJobInfo(jarInfo, true, JobStatus.Running, Some(contextId))
      val finishedJob2 = genJobInfo(jarInfo, true, JobStatus.Error, Some(contextId))
      helperJobSqlDao.saveJobInfo(finishedJob)
      helperJobSqlDao.saveJobInfo(jobWithDifferentContextId)
      helperJobSqlDao.saveJobInfo(runningJob)
      helperJobSqlDao.saveJobInfo(finishedJob2)

      val retrieved = Await.result(dao.getJobsByContextId(contextId), timeout)
      retrieved.size shouldBe 3
      retrieved.filter(_.contextId == contextId).size shouldBe 3
      retrieved.map(_.jobId) should contain allOf(finishedJob.jobId, runningJob.jobId, finishedJob2.jobId)
    }

    it("should retrieve finished and running jobs by context id") {
      val contextId = UUID.randomUUID().toString()
      val runningJob = genJobInfo(jarInfo, true, JobStatus.Running, Some(contextId))
      val errorJob = genJobInfo(jarInfo, true, JobStatus.Error, Some(contextId))
      val finishedJob = genJobInfo(jarInfo, true, JobStatus.Finished, Some(contextId))

      helperJobSqlDao.saveJobInfo(runningJob)
      helperJobSqlDao.saveJobInfo(errorJob)
      helperJobSqlDao.saveJobInfo(finishedJob)

      val retrieved = Await.result(
          dao.getJobsByContextId(contextId, Some(Seq(JobStatus.Running, JobStatus.Finished))), timeout)
      retrieved.size shouldBe 2
      retrieved should contain allElementsOf(List(runningJob, finishedJob))
    }

    it("should retrieve nothing if no job with the specified status exist") {
      val contextId = UUID.randomUUID().toString()
      val runningJob = genJobInfo(jarInfo, true, JobStatus.Running, Some(contextId))

      helperJobSqlDao.saveJobInfo(runningJob)

      val retrieved = Await.result(
          dao.getJobsByContextId(contextId, Some(Seq(JobStatus.Killed, JobStatus.Finished))), timeout)
      retrieved shouldBe empty
    }
  }

  describe("Test saveContextInfo(), getContextInfo(), getContextInfos() and getContextInfoByName()") {
    it("should return None if context doesnot exist in Context table") {
      None should equal (Await.result(dao.getContext("dummy-id"), timeout))
    }

    it("should save a new ContextInfo and get the same ContextInfo") {
      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContext(dummyContext)

      val context = Await.result(dao.getContext("someId"), timeout)
      context.head should equal (dummyContext)
    }

    it("should get the latest ContextInfo by name") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContext(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now().plusHours(2), None,
          ContextStatus.Started, None)
      dao.saveContext(dummyContext)

      val context = Await.result(dao.getContextByName("contextName"), timeout)
      context.head should equal (dummyContext)
    }

    it("should get all ContextInfos if no limit is given") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now().plusHours(2),
          None, ContextStatus.Finished, None)
      dao.saveContext(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now().plusHours(1), None,
          ContextStatus.Finished, None)
      dao.saveContext(dummyContext)

      val dummyContextNew = ContextInfo("someIdNew", "contextNameNew", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContext(dummyContextNew)

      val contexts: Seq[ContextInfo] = Await.result(dao.getContexts(), timeout)
      contexts.size should equal (3)
      contexts should contain theSameElementsAs Seq(dummyContextOld, dummyContext, dummyContextNew)
    }

    it("should get the 2 latest ContextInfos for limit = 2") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContext(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now().plusHours(1), None,
          ContextStatus.Finished, None)
      dao.saveContext(dummyContext)

      val dummyContextNew = ContextInfo("someIdNew", "contextNameNew", "", None, DateTime.now().plusHours(2),
          None, ContextStatus.Started, None)
      dao.saveContext(dummyContextNew)

      val contexts: Seq[ContextInfo] = Await.result(dao.getContexts(Some(2)), timeout)
      contexts.size should equal (2)
      contexts should contain theSameElementsAs Seq(dummyContext, dummyContextNew)
    }

    it("should get ContextInfos of all contexts with state FINISHED") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now().plusHours(2),
          None, ContextStatus.Finished, None)
      dao.saveContext(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now().plusHours(1), None,
          ContextStatus.Finished, None)
      dao.saveContext(dummyContext)

      val dummyContextNew = ContextInfo("someIdNew", "contextNameNew", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContext(dummyContextNew)

      val contexts: Seq[ContextInfo] = Await.result(dao.getContexts(
          None, Some(Seq(ContextStatus.Finished))), timeout)
      contexts.size should equal (2)
      contexts should contain theSameElementsAs Seq(dummyContextOld, dummyContext)
    }

    it("should get ContextInfos of all contexts with state Finished OR Running") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now().plusHours(2),
          None, ContextStatus.Finished, None)
      dao.saveContext(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now().plusHours(1), None,
          ContextStatus.Finished, None)
      dao.saveContext(dummyContext)

      val dummyContextNew = ContextInfo("someIdNew", "contextNameNew", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContext(dummyContextNew)

      val dummyContextRestarting = ContextInfo("someIdRestarting", "contextNameRestarting", "", None,
          DateTime.now(), None, ContextStatus.Restarting, None)
      dao.saveContext(dummyContextRestarting)

      val contexts: Seq[ContextInfo] = Await.result(dao.getContexts(
          None, Some(Seq(ContextStatus.Restarting, ContextStatus.Started))), timeout)
      contexts.size should equal (2)
      contexts should contain theSameElementsAs Seq(dummyContextRestarting, dummyContextNew)
    }

    it("should update the context, if id is the same for two saveContextInfo requests") {
      val dummyContext = ContextInfo("context2", "dummy-name", "", None, DateTime.now().plusHours(1), None,
          ContextStatus.Started, None)
      dao.saveContext(dummyContext)

      val dummyContext2 = ContextInfo("context2", "new-name", "", Some("akka:tcp//test"),
          DateTime.now(), None, ContextStatus.Running, None)
      dao.saveContext(dummyContext2)

      val context = Await.result(dao.getContext("context2"), timeout)
      context.head should equal (dummyContext2)
    }

    it("ContextInfo should still exist after db restart") {
      val dummyContext = ContextInfo("context3", "dummy-name", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContext(dummyContext)

      // Destroy and bring up the DB again
      helperJobSqlDao = null
      helperJobSqlDao = new JobSqlDAO(config)
      val context = Await.result(dao.getContext("context3"), timeout)

      context.head should equal (dummyContext)
    }
  }

}