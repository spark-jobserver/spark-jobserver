package spark.jobserver.io

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import java.io.File

import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
import spark.jobserver.TestJarFinder
import java.util.UUID

import org.mockito.Mockito
import slick.SlickException

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
      jarInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_HHmmss_SSS") + ".jar"
  )

  val eggBytes: Array[Byte] = Files.toByteArray(emptyEgg)
  val eggInfo: BinaryInfo = BinaryInfo("myEggBinary", BinaryType.Egg, time)
  val eggFile: File = new File(config.getString("spark.jobserver.sqldao.rootdir"),
    eggInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_HHmmss_SSS") + ".egg")

  // jobInfo test data
  val jobInfoNoEndNoErr:JobInfo = genJobInfo(jarInfo, false, JobStatus.Running)
  val expectedJobInfo = jobInfoNoEndNoErr
  val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, false, JobStatus.Finished)
  val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, false, ContextStatus.Error)

  // job config test data
  val jobId: String = jobInfoNoEndNoErr.jobId

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

  case class GenJobInfoClosure() {
    var count: Int = 0

    def apply(jarInfo: BinaryInfo, isNew: Boolean, state: String,
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
      val someEndTime: Option[DateTime] = Some(time) // Any DateTime Option is fine
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

  before {
    dao = new JobSqlDAO(config)
    jarFile.delete()
    eggFile.delete()
  }

  describe("verify initial setup") {
    it("should create root dir folder on initialization") {
      val dummyRootDir = "/tmp/dummy"

      val rootDir = new File(dummyRootDir)
      rootDir.exists() should be (false)

      dao = new JobSqlDAO(config.withValue("spark.jobserver.sqldao.rootdir",
        ConfigValueFactory.fromAnyRef(dummyRootDir)))

      rootDir.exists() should be (true)

      rootDir.delete() // cleanup
    }
  }

  describe("save and get the jars") {
    it("should be able to save one jar and get it back") {
      jarFile.exists() should equal (false)

      dao.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)
      val apps: Map[String, (BinaryType, DateTime)] =
        Await.result(dao.getApps, timeout).filter(_._2._1 == BinaryType.Jar)

      jarFile.exists() should equal (true)
      apps.keySet should equal (Set(jarInfo.appName))
      apps(jarInfo.appName) should equal ((BinaryType.Jar, jarInfo.uploadTime))
    }

    it("should be able to save one jar and get it back without creating a cache") {
      val configNoCache = config.withValue("spark.jobserver.cache-on-upload", ConfigValueFactory.fromAnyRef(false))
      val daoNoCache = new JobSqlDAO(configNoCache)

      jarFile.exists() should equal (false)
      daoNoCache.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)
      val apps: Map[String, (BinaryType, DateTime)] =
        Await.result(daoNoCache.getApps, timeout).filter(_._2._1 == BinaryType.Jar)

      jarFile.exists() should equal (false)
      apps.keySet should equal (Set(jarInfo.appName))
      apps(jarInfo.appName) should equal ((BinaryType.Jar, jarInfo.uploadTime))
    }

    it("should be able to retrieve the jar file") {
      jarFile.exists() should equal (false)
      val jarFilePath: String = dao.getBinaryFilePath(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime)

      jarFile.exists() should equal (true)
      jarFilePath should equal (jarFile.getAbsolutePath)
    }
  }

  describe("save and get Python eggs") {
    it("should be able to save one egg and get it back") {
      eggFile.exists() should equal (false)
      dao.saveBinary(eggInfo.appName, BinaryType.Egg, eggInfo.uploadTime, eggBytes)
      val apps: Map[String, (BinaryType, DateTime)] =
        Await.result(dao.getApps, timeout).filter(_._2._1 == BinaryType.Egg)

      eggFile.exists() should equal (true)
      apps.keySet should equal (Set(eggInfo.appName))
      apps(eggInfo.appName) should equal ((BinaryType.Egg, eggInfo.uploadTime))
    }

    it("should be able to retrieve the egg file") {
      eggFile.exists() should equal (false)
      val eggFilePath: String = dao.getBinaryFilePath(eggInfo.appName, BinaryType.Egg, eggInfo.uploadTime)

      eggFile.exists() should equal (true)
      eggFilePath should equal (eggFile.getAbsolutePath)
    }
  }

  describe("Basic saveJobInfo() and getJobInfos() tests") {
    it("Save another new jobInfo, bring down DB, bring up DB, should JobInfos from DB") {
      dao.saveJobInfo(jobInfoNoEndNoErr)
      val jobInfo2 = genJobInfo(jarInfo, true, JobStatus.Running)
      val jobId2 = jobInfo2.jobId
      val expectedJobInfo2 = jobInfo2

      dao.saveJobInfo(jobInfo2)

      // Destroy and bring up the DB again
      dao = null
      dao = new JobSqlDAO(config)

      val jobs = Await.result(dao.getJobInfos(2), timeout)
      val jobIds = jobs map { _.jobId }

      jobIds should equal (Seq(jobId2, jobId))
      jobs should equal (Seq(expectedJobInfo2, expectedJobInfo))
    }

    it("saving a JobInfo with the same jobId should update the JOBS table") {
      val expectedNoEndNoErr = jobInfoNoEndNoErr
      val expectedSomeEndNoErr = jobInfoSomeEndNoErr
      val expectedSomeEndSomeErr = jobInfoSomeEndSomeErr
      val exJobId = jobInfoNoEndNoErr.jobId

      val info = genJarInfo(true, false)
      info.uploadTime should equal (jarInfo.uploadTime)

      val jobs: Seq[JobInfo] = Await.result(dao.getJobInfos(2), timeout)

      jobs.size should equal (2)
      jobs.last should equal (expectedJobInfo)

      // Cannot compare JobInfos directly if error is a Some(Throwable) because
      // Throwable uses referential equality
      dao.saveJobInfo(expectedNoEndNoErr)
      val jobs2 = Await.result(dao.getJobInfos(2), timeout)
      jobs2.size should equal (2)
      jobs2.last.endTime should equal (None)
      jobs2.last.error shouldBe None

      dao.saveJobInfo(jobInfoSomeEndNoErr)
      val jobs3 = Await.result(dao.getJobInfos(2), timeout)
      jobs3.size should equal (2)
      jobs3.last.error.isDefined should equal (false)
      jobs3.last should equal (expectedSomeEndNoErr)

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

    it("clean running jobs for context") {
      val ctxToBeCleaned: JobInfo = JobInfo(
          "jobId", UUID.randomUUID().toString(), "context", jarInfo,
          "test-class", JobStatus.Running, DateTime.now(), None, None)
      dao.saveJobInfo(ctxToBeCleaned)

      Await.ready(dao.cleanRunningJobInfosForContext(ctxToBeCleaned.contextId, DateTime.now()), timeout)

      val jobInfo = Await.result(dao.getJobInfo(ctxToBeCleaned.jobId), timeout).get
      jobInfo.endTime shouldBe defined
      jobInfo.error shouldBe defined
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

  describe("saveContextInfo tests") {
    it("should throw an exception if save was unsuccessful") {
      val dummyContext = ContextInfo("someId", "contextName", "", None, DateTime.now(), None,
        ContextStatus.Started, None)
      val sqlCommonMock = Mockito.spy(new SqlCommon(config))
      val mockedDao = new JobSqlDAO(config, sqlCommonMock)

      Mockito.when(sqlCommonMock.saveContext(dummyContext)).thenReturn(Future{false})

      intercept[SlickException] {
        mockedDao.saveContextInfo(dummyContext)
      }
    }
  }

  describe("saveJobConfig tests") {
    it("should throw an exception if save was unsuccessful") {
      val jobId: String = jobInfoNoEndNoErr.jobId
      val jobConfig: Config = ConfigFactory.parseString("{marco=pollo}")

      val sqlCommonMock = Mockito.spy(new SqlCommon(config))
      val mockedDao = new JobSqlDAO(config, sqlCommonMock)

      Mockito.when(sqlCommonMock.saveJobConfig(jobId, jobConfig)).thenReturn(Future{false})

      intercept[SlickException] {
        mockedDao.saveJobConfig(jobId, jobConfig)
      }
    }
  }
}

class JobSqlDAODBCPSpec extends JobSqlDAOSpec {
  override def config: Config = ConfigFactory.load("local.test.jobsqldao_dbcp.conf")
}
