package spark.jobserver.io

import scala.concurrent.Await
import scala.concurrent.duration._

import java.io.File

import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
import spark.jobserver.TestJarFinder

abstract class JobSqlDAOSpecBase {
  def config : Config
}

class JobSqlDAOSpec extends JobSqlDAOSpecBase with TestJarFinder with FunSpecLike with Matchers
  with BeforeAndAfter {
  override def config: Config = ConfigFactory.load("local.test.jobsqldao.conf")
  val timeout = 60 seconds
  var dao: JobSqlDAO = _

  // *** TEST DATA ***
  val time: DateTime = new DateTime()
  val throwable: Throwable = new Throwable("test-error")
  // jar test data
  val jarInfo: BinaryInfo = genJarInfo(false, false)
  val jarBytes: Array[Byte] = Files.toByteArray(testJar)
  var jarFile: File = new File(
      config.getString("spark.jobserver.sqldao.rootdir"),
      jarInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".jar"
  )

  val eggBytes: Array[Byte] = Files.toByteArray(emptyEgg)
  val eggInfo: BinaryInfo = BinaryInfo("myEggBinary", BinaryType.Egg, time)
  val eggFile: File = new File(config.getString("spark.jobserver.sqldao.rootdir"),
    eggInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".egg")

  // jobInfo test data
  val jobInfoNoEndNoErr:JobInfo = genJobInfo(jarInfo, false, false, false)
  val expectedJobInfo = jobInfoNoEndNoErr
  val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, true, false, false)
  val jobInfoNoEndSomeErr: JobInfo = genJobInfo(jarInfo, false, true, false)
  val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, true, true, false)

  // job config test data
  val jobId: String = jobInfoNoEndNoErr.jobId
  val jobConfig: Config = ConfigFactory.parseString("{marco=pollo}")
  val expectedConfig: Config =
    ConfigFactory.empty().withValue("marco", ConfigValueFactory.fromAnyRef("pollo"))

  // Helper functions and closures!!
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

  private def genJobInfoClosure = {
    var count: Int = 0

    def genTestJobInfo(jarInfo: BinaryInfo,
                       hasEndTime: Boolean,
                       hasError: Boolean,
                       isNew:Boolean): JobInfo = {
      count = count + (if (isNew) 1 else 0)
      val id: String = "test-id" + count
      val contextName: String = "test-context"
      val classPath: String = "test-classpath"
      val startTime: DateTime = time

      val noEndTime: Option[DateTime] = None
      val someEndTime: Option[DateTime] = Some(time) // Any DateTime Option is fine
      val someError = Some(ErrorData(throwable))

      val endTime: Option[DateTime] = if (hasEndTime) someEndTime else noEndTime
      val error = if (hasError) someError else None

      JobInfo(id, contextName, jarInfo, classPath, startTime, endTime, error)
    }

    genTestJobInfo _
  }

  def genJarInfo: (Boolean, Boolean) => BinaryInfo = genJarInfoClosure
  def genJobInfo: (BinaryInfo, Boolean, Boolean, Boolean) => JobInfo = genJobInfoClosure
  //**********************************

  before {
    dao = new JobSqlDAO(config)
    jarFile.delete()
    eggFile.delete()
  }

  describe("save and get the jars") {
    it("should be able to save one jar and get it back") {
      // check the pre-condition
      jarFile.exists() should equal (false)

      // save
      dao.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)

      // read it back
      val apps: Map[String, (BinaryType, DateTime)] =
        Await.result(dao.getApps, timeout).filter(_._2._1 == BinaryType.Jar)

      // test
      jarFile.exists() should equal (true)
      apps.keySet should equal (Set(jarInfo.appName))
      apps(jarInfo.appName) should equal ((BinaryType.Jar, jarInfo.uploadTime))
    }

    it("should be able to save one jar and get it back without creating a cache") {
      val configNoCache = config.withValue("spark.jobserver.cache-on-upload", ConfigValueFactory.fromAnyRef(false))
      val daoNoCache = new JobSqlDAO(configNoCache)
      // check the pre-condition
      jarFile.exists() should equal (false)

      // save
      daoNoCache.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)

      // read it back
      val apps: Map[String, (BinaryType, DateTime)] =
        Await.result(daoNoCache.getApps, timeout).filter(_._2._1 == BinaryType.Jar)

      // test
      jarFile.exists() should equal (false)
      apps.keySet should equal (Set(jarInfo.appName))
      apps(jarInfo.appName) should equal ((BinaryType.Jar, jarInfo.uploadTime))
    }

    it("should be able to retrieve the jar file") {
      // check the pre-condition
      jarFile.exists() should equal (false)

      // retrieve the jar file
      val jarFilePath: String = dao.retrieveBinaryFile(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime)

      // test
      jarFile.exists() should equal (true)
      jarFilePath should equal (jarFile.getAbsolutePath)
    }
  }

  describe("save and get Python eggs") {
    it("should be able to save one egg and get it back") {

      // check the pre-condition
      eggFile.exists() should equal (false)

      // save
      dao.saveBinary(eggInfo.appName, BinaryType.Egg, eggInfo.uploadTime, eggBytes)

      // read it back
      val apps: Map[String, (BinaryType, DateTime)] =
        Await.result(dao.getApps, timeout).filter(_._2._1 == BinaryType.Egg)

      // test
      eggFile.exists() should equal (true)
      apps.keySet should equal (Set(eggInfo.appName))
      apps(eggInfo.appName) should equal ((BinaryType.Egg, eggInfo.uploadTime))
    }

    it("should be able to retrieve the egg file") {
      // check the pre-condition
      eggFile.exists() should equal (false)

      // retrieve the jar file
      val eggFilePath: String = dao.retrieveBinaryFile(eggInfo.appName, BinaryType.Egg, eggInfo.uploadTime)
      // test
      eggFile.exists() should equal (true)
      eggFilePath should equal (eggFile.getAbsolutePath)
    }
  }

  describe("saveJobConfig() tests") {
    it("should provide None on getJobConfig(jobId) where there is no config for a given jobId") {
      val config = Await.result(dao.getJobConfig("44c32fe1-38a4-11e1-a06a-485d60c81a3e"), timeout)
      config shouldBe None
    }

    it("should save and get the same config") {
      // save job config
      dao.saveJobConfig(jobId, jobConfig)

      // get all configs
      val config = Await.result(dao.getJobConfig(jobId), timeout).get

      // test
      config should equal (expectedConfig)
    }

    it("should be able to get previously saved config") {
      // config saved in prior test

      val config = Await.result(dao.getJobConfig(jobId), timeout).get

      // test
      config should equal (expectedConfig)
    }

    it("Save a new config, bring down DB, bring up DB, should get configs from DB") {
      val jobId2: String = genJobInfo(genJarInfo(false, false), false, false, true).jobId
      val jobConfig2: Config = ConfigFactory.parseString("{merry=xmas}")
      val expectedConfig2 = ConfigFactory.empty().withValue("merry", ConfigValueFactory.fromAnyRef("xmas"))
      // config previously saved

      // save new job config
      dao.saveJobConfig(jobId2, jobConfig2)

      // Destroy and bring up the DB again
      dao = null
      dao = new JobSqlDAO(config)

      // Get all configs
      val jobIdConfig = Await.result(dao.getJobConfig(jobId), timeout).get
      val jobId2Config = Await.result(dao.getJobConfig(jobId2), timeout).get

      // test
      jobIdConfig should equal (expectedConfig)
      jobId2Config should equal (expectedConfig2)
    }
  }

  describe("Basic saveJobInfo() and getJobInfos() tests") {
    it("should provide an empty Seq on getJobInfos() for an empty JOBS table") {
      Seq.empty[JobInfo] should equal (Await.result(dao.getJobInfos(1), timeout))
    }

    it("should save a new JobInfo and get the same JobInfo") {
      // save JobInfo
      dao.saveJobInfo(jobInfoNoEndNoErr)

      // get some JobInfos
      val jobs: Seq[JobInfo] = Await.result(dao.getJobInfos(10), timeout)

      jobs.head.jobId should equal (jobId)
      jobs.head should equal (expectedJobInfo)
    }

    it("should be able to get previously saved JobInfo") {
      // jobInfo saved in prior test

      // get jobInfos
      val jobInfo = Await.result(dao.getJobInfo(jobId), timeout).get

      // test
      jobInfo should equal (expectedJobInfo)
    }

    it("Save another new jobInfo, bring down DB, bring up DB, should JobInfos from DB") {
      val jobInfo2 = genJobInfo(jarInfo, false, false, true)
      val jobId2 = jobInfo2.jobId
      val expectedJobInfo2 = jobInfo2
      // jobInfo previously saved

      // save new job config
      dao.saveJobInfo(jobInfo2)

      // Destroy and bring up the DB again
      dao = null
      dao = new JobSqlDAO(config)

      // Get jobInfos
      val jobs = Await.result(dao.getJobInfos(2), timeout)
      val jobIds = jobs map { _.jobId }

      // test
      jobIds should equal (Seq(jobId2, jobId))
      jobs should equal (Seq(expectedJobInfo2, expectedJobInfo))
    }

    it("saving a JobInfo with the same jobId should update the JOBS table") {
      val expectedNoEndSomeErr = jobInfoNoEndSomeErr
      val expectedSomeEndNoErr = jobInfoSomeEndNoErr
      val expectedSomeEndSomeErr = jobInfoSomeEndSomeErr
      val exJobId = jobInfoNoEndNoErr.jobId

      val info = genJarInfo(true, false)
      info.uploadTime should equal (jarInfo.uploadTime)

      // Get all jobInfos
      val jobs: Seq[JobInfo] = Await.result(dao.getJobInfos(2), timeout)

      // First Test
      jobs.size should equal (2)
      jobs.last should equal (expectedJobInfo)

      // Second Test
      // Cannot compare JobInfos directly if error is a Some(Throwable) because
      // Throwable uses referential equality
      dao.saveJobInfo(jobInfoNoEndSomeErr)
      val jobs2 = Await.result(dao.getJobInfos(2), timeout)
      jobs2.size should equal (2)
      jobs2.last.endTime should equal (None)
      jobs2.last.error shouldBe defined
      jobs2.last.error.get.message should equal (throwable.getMessage)
      jobs2.last.error.get.errorClass should equal (throwable.getClass.getName)
      jobs2.last.error.get.stackTrace should not be empty

      // Third Test
      dao.saveJobInfo(jobInfoSomeEndNoErr)
      val jobs3 = Await.result(dao.getJobInfos(2), timeout)
      jobs3.size should equal (2)
      jobs3.last.error.isDefined should equal (false)
      jobs3.last should equal (expectedSomeEndNoErr)

      // Fourth Test
      // Cannot compare JobInfos directly if error is a Some(Throwable) because
      // Throwable uses referential equality
      dao.saveJobInfo(jobInfoSomeEndSomeErr)
      val jobs4 = Await.result(dao.getJobInfos(2), timeout)
      jobs4.size should equal (2)
      jobs4.last.endTime should equal (expectedSomeEndSomeErr.endTime)
      jobs4.last.error shouldBe defined
      jobs4.last.error.get.message should equal (throwable.getMessage)
      jobs4.last.error.get.errorClass should equal (throwable.getClass.getName)
      jobs4.last.error.get.stackTrace should not be empty
    }
    it("retrieve by status equals running should be no end and no error") {
      //save some job insure exist one running job
      val dt1 = DateTime.now()
      val dt2 = Some(DateTime.now())
      val someError = Some(ErrorData("test-error", "", ""))
      val finishedJob: JobInfo = JobInfo("test-finished", "test", jarInfo, "test-class", dt1, dt2, None)
      val errorJob: JobInfo = JobInfo("test-error", "test", jarInfo, "test-class", dt1, dt2, someError)
      val runningJob: JobInfo = JobInfo("test-running", "test", jarInfo, "test-class", dt1, None, None)
      val runningJobWithContext: JobInfo = JobInfo("test-running-with-context", "context", jarInfo, "test-class", dt1, None, None)
      dao.saveJobInfo(finishedJob)
      dao.saveJobInfo(runningJob)
      dao.saveJobInfo(runningJobWithContext)
      dao.saveJobInfo(errorJob)

      //retrieve by status equals RUNNING
      val retrieved = Await.result(dao.getJobInfos(1, Some(JobStatus.Running)), timeout).head

      //test
      retrieved.endTime.isDefined should equal (false)
      retrieved.error.isDefined should equal (false)
    }
    it("retrieve by status equals finished should be some end and no error") {

      //retrieve by status equals FINISHED
      val retrieved = Await.result(dao.getJobInfos(1, Some(JobStatus.Finished)), timeout).head

      //test
      retrieved.endTime.isDefined should equal (true)
      retrieved.error.isDefined should equal (false)
    }

    it("retrieve by status equals error should be some error") {
      //retrieve by status equals ERROR
      val retrieved = Await.result(dao.getJobInfos(1, Some(JobStatus.Error)), timeout).head

      //test
      retrieved.error.isDefined should equal (true)
    }

    it("retrieve running jobs by cluster name") {
      val retrieved = Await.result(dao.getRunningJobInfosForContextName("context"), timeout)

      //test
      retrieved.size shouldBe 1
      retrieved.head.contextName shouldBe "context"
    }

    it("clean running jobs for context") {
      Await.ready(dao.cleanRunningJobInfosForContext("context", DateTime.now()), timeout)

      val jobInfo = Await.result(dao.getJobInfo("test-running-with-context"), timeout).get
      jobInfo.endTime shouldBe defined
      jobInfo.error shouldBe defined
    }
  }

  describe("Test saveContextInfo(), getContextInfo(), getContextInfos() and getContextInfoByName()") {
    it("should return None if context doesnot exist in Context table") {
      None should equal (Await.result(dao.getContextInfo("dummy-id"), timeout))
    }

    it("should save a new ContextInfo and get the same ContextInfo") {
      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContextInfo(dummyContext)

      val context = Await.result(dao.getContextInfo("someId"), timeout)
      context.head should equal (dummyContext)
    }

    it("should get the latest ContextInfo by name") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContextInfo(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContextInfo(dummyContext)

      val context = Await.result(dao.getContextInfoByName("contextName"), timeout)
      context.head should equal (dummyContext)
    }

    it("should get all ContextInfos if no limit is given") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContextInfo(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContextInfo(dummyContext)

      val dummyContextNew = ContextInfo("someIdNew", "contextNameNew", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContextInfo(dummyContextNew)

      val contexts: Seq[ContextInfo] = Await.result(dao.getContextInfos(), timeout)
      contexts.size should equal (3)
      contexts should contain theSameElementsAs Seq(dummyContextOld, dummyContext, dummyContextNew)
    }

    it("should get the 2 latest ContextInfos for limit = 2") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContextInfo(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContextInfo(dummyContext)

      val dummyContextNew = ContextInfo("someIdNew", "contextNameNew", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContextInfo(dummyContextNew)

      val contexts: Seq[ContextInfo] = Await.result(dao.getContextInfos(Some(2)), timeout)
      contexts.size should equal (2)
      contexts should contain theSameElementsAs Seq(dummyContext, dummyContextNew)
    }

    it("should get ContextInfos all contexts with state FINISHED") {
      val dummyContextOld = ContextInfo("someIdOld", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContextInfo(dummyContextOld)

      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now(), None,
          ContextStatus.Finished, None)
      dao.saveContextInfo(dummyContext)

      val dummyContextNew = ContextInfo("someIdNew", "contextNameNew", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContextInfo(dummyContextNew)

      val contexts: Seq[ContextInfo] = Await.result(dao.getContextInfos(None, Some(ContextStatus.Finished)), timeout)
      contexts.size should equal (2)
      contexts should contain theSameElementsAs Seq(dummyContextOld, dummyContext)
    }

    it("should update the context, if id is the same for two saveContextInfo requests") {
      val dummyContext = ContextInfo("context2", "dummy-name", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContextInfo(dummyContext)

      val dummyContext2 = ContextInfo("context2", "new-name", "", Some("akka:tcp//test"),
          DateTime.now(), None, ContextStatus.Running, None)
      dao.saveContextInfo(dummyContext2)

      val context = Await.result(dao.getContextInfo("context2"), timeout)
      context.head should equal (dummyContext2)
    }

    it("ContextInfo should still exist after db restart") {
      val dummyContext = ContextInfo("context3", "dummy-name", "", None, DateTime.now(), None,
          ContextStatus.Started, None)
      dao.saveContextInfo(dummyContext)

      // Destroy and bring up the DB again
      dao = null
      dao = new JobSqlDAO(config)
      val context = Await.result(dao.getContextInfo("context3"), timeout)

      context.head should equal (dummyContext)
    }
  }

  describe("delete binaries") {
    it("should be able to delete jar file") {
      val existing = Await.result(dao.getApps, timeout)
      existing.keys should contain (jarInfo.appName)
      dao.deleteBinary(jarInfo.appName)

      val apps = Await.result(dao.getApps, timeout)
      apps.keys should not contain (jarInfo.appName)
    }
  }

}

class JobSqlDAODBCPSpec extends JobSqlDAOSpec {
  override def config: Config = ConfigFactory.load("local.test.jobsqldao_dbcp.conf")
}
