package spark.jobserver.io

import akka.http.scaladsl.model.Uri

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.File
import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
import spark.jobserver.TestJarFinder
import spark.jobserver.util.{ErrorData, JobserverConfig}

abstract class MetaDataSqlDAOSpecBase {
  def config : Config
}

class MetaDataSqlDAOSpec extends MetaDataSqlDAOSpecBase with TestJarFinder with FunSpecLike with Matchers
  with BeforeAndAfter {
  override def config: Config = ConfigFactory.load("local.test.metadatasqldao.conf")
  val timeout = 60 seconds
  var dao: MetaDataSqlDAO = _

  // *** TEST DATA ***
  val time: DateTime = new DateTime()
  val throwable: Throwable = new Throwable("test-error")
  // jar test data
  val jarBytes: Array[Byte] = Files.toByteArray(testJar)
  val jarInfo: BinaryInfo = genJarInfo(false, false)
  var jarFile: File = new File(
      config.getString(JobserverConfig.DAO_ROOT_DIR_PATH),
      jarInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".jar"
  )

  val eggBytes: Array[Byte] = Files.toByteArray(emptyEgg)
  val eggInfo: BinaryInfo = BinaryInfo("myEggBinary", BinaryType.Egg, time)
  val eggFile: File = new File(config.getString(JobserverConfig.DAO_ROOT_DIR_PATH),
    eggInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".egg")

  val wheelBytes: Array[Byte] = Files.toByteArray(emptyWheel)
  val wheelInfo: BinaryInfo = BinaryInfo("myWheelBinary", BinaryType.Wheel, time)
  val wheelFile: File = new File(config.getString(JobserverConfig.DAO_ROOT_DIR_PATH),
    wheelInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".whl")

  // jobInfo test data
  val jobInfoNoEndNoErr: JobInfo = genJobInfo(jarInfo, false, JobStatus.Running)
  val expectedJobInfo: JobInfo = jobInfoNoEndNoErr
  val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, false, JobStatus.Finished)
  val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, false, ContextStatus.Error)
  val jobInfoSomeCallback: JobInfo = genJobInfo(jarInfo, false, JobStatus.Finished,
    None, Some(Uri("http://example.com")))

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

      BinaryInfo(app, BinaryType.Jar, upload, Some(BinaryDAO.calculateBinaryHashString(jarBytes)))
    }

    genTestJarInfo _
  }

  case class GenJobInfoClosure() {
    var count: Int = 0

    def apply(jarInfo: BinaryInfo, isNew: Boolean, state: String,
        contextId: Option[String] = None, callbackUrl: Option[Uri] = None): JobInfo = {
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

      JobInfo(id, ctxId, contextName, classPath, state, startTime,
          endTimeAndError._1, endTimeAndError._2, Seq(jarInfo), callbackUrl)
    }
  }

  def genJarInfo: (Boolean, Boolean) => BinaryInfo = genJarInfoClosure
  lazy val genJobInfo = GenJobInfoClosure()

  before {
    dao = new MetaDataSqlDAO(config)
  }

  after {
    val profile = dao.dbUtils.profile
    import profile.api._
    Await.result( {
      for {
        bins <- dao.dbUtils.db.run(dao.binaries.delete)
        cont <- dao.dbUtils.db.run(dao.contexts.delete)
        jobs <- dao.dbUtils.db.run(dao.jobs.delete)
        conf <- dao.dbUtils.db.run(dao.configs.delete)
      } yield {
        bins + cont + jobs + conf
      }
    }, timeout)
  }

  describe("binaries") {
    it("should be able to save one jar and get it back") {
      val save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
            jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)

      val binary = Await.result(dao.getBinary(jarInfo.appName), timeout).get

      binary.uploadTime should equal (jarInfo.uploadTime)
      binary.binaryType should equal (BinaryType.Jar)
    }

    it("should be able to save one egg and get it back") {
      val save = Await.result(dao.saveBinary(eggInfo.appName, BinaryType.Egg, eggInfo.uploadTime,
          BinaryDAO.calculateBinaryHashString(eggBytes)), timeout)
      save should equal (true)
      val binary = Await.result(dao.getBinary(eggInfo.appName), timeout)

      binary.get.uploadTime should equal (eggInfo.uploadTime)
      binary.get.binaryType should equal (BinaryType.Egg)
    }

    it("should be able to save one wheel and get it back") {
      val save = Await.result(dao.saveBinary(wheelInfo.appName, BinaryType.Wheel, wheelInfo.uploadTime,
        BinaryDAO.calculateBinaryHashString(wheelBytes)), timeout)
      save should equal (true)
      val binary = Await.result(dao.getBinary(wheelInfo.appName), timeout)

      binary.get.uploadTime should equal (wheelInfo.uploadTime)
      binary.get.binaryType should equal (BinaryType.Wheel)
    }

    it("should be able to get binaries with the same hash") {
      var save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
            jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      val time = new DateTime()
      save = Await.result(dao.saveBinary("new_name", BinaryType.Jar,
            time, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      save = Await.result(dao.saveBinary("other_name", BinaryType.Jar, time,
            BinaryDAO.calculateBinaryHashString(Array(1.toByte))), timeout)
      save should equal (true)
      val binaries =
        Await.result(dao.getBinariesByStorageId(BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)

      binaries.size should equal (2)
      val names = binaries map { _.appName }
      names should equal (Seq(jarInfo.appName, "new_name"))
    }

    it("should be able to get binaries") {
      var save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
            jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      val time = new DateTime()
      save = Await.result(dao.saveBinary("new_name", BinaryType.Jar,
            time, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      save = Await.result(dao.saveBinary("other_name", BinaryType.Jar, time.plusHours(1),
            BinaryDAO.calculateBinaryHashString(Array(1.toByte))), timeout)
      save should equal (true)
      val binaries = Await.result(dao.getBinaries, timeout)

      binaries.size should equal (3)
      val names = binaries map { _.appName }
      names should equal (List("new_name", "other_name", jarInfo.appName))
    }

    it("should be able to get the newest binary") {
      val time = new DateTime()
      val newInfo = BinaryInfo("name", BinaryType.Jar, time.plusHours(1),
          Some(BinaryDAO.calculateBinaryHashString(jarBytes)))
      var save = Await.result(dao.saveBinary("name", BinaryType.Jar, time.plusHours(1),
          BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      save = Await.result(dao.saveBinary("name", BinaryType.Jar, time,
            BinaryDAO.calculateBinaryHashString(Array(1.toByte))), timeout)
      save should equal (true)
      val binaries = Await.result(dao.getBinary("name"), timeout)

      binaries.size should equal (1)
      val names = binaries map { _.appName }
      binaries.head should equal (newInfo)
    }

    it("should be able to delete binary") {
      Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime,
          BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      val res = Await.result(dao.deleteBinary(jarInfo.appName), timeout)
      res should equal (true)

      val binary = Await.result(dao.getBinary(jarInfo.appName), timeout)
      binary.isEmpty should equal (true)
    }

    it("should be able to delete multiple binaries with the same name") {
      var save = Await.result(dao.saveBinary("name", BinaryType.Jar, time.plusHours(1),
          BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      save = Await.result(dao.saveBinary("name", BinaryType.Jar, time,
            BinaryDAO.calculateBinaryHashString(Array(1.toByte))), timeout)
      save should equal (true)

      val del = Await.result(dao.deleteBinary("name"), timeout)
      del should equal (true)

      val binaries = Await.result(dao.getBinary("name"), timeout)
      binaries.size should equal (0)
    }

    it("should return false if there is nothing to delete") {
      val binaries = Await.result(dao.getBinary("name"), timeout)
      binaries.size should equal (0)

      val del = Await.result(dao.deleteBinary("name"), timeout)
      del should equal (false)
    }
  }

  describe("Basic saveJobInfo() and getJobInfos() tests") {
    it("Save jobInfo and get JobInfos from DB") {
      var save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
            jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      val id = jobInfoNoEndNoErr.jobId
      save = Await.result(dao.saveJob(jobInfoNoEndNoErr), timeout)
      save should equal (true)
      val job = Await.result(dao.getJob(id), timeout).get

      job should equal (jobInfoNoEndNoErr)
    }

    it("Save jobInfo, bring down DB, bring up DB, should get JobInfos from DB") {
      var save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
            jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      save = Await.result(dao.saveJob(jobInfoNoEndNoErr), timeout)
      save should equal (true)
      val jobInfo2 = genJobInfo(jarInfo, true, JobStatus.Running)
      val jobId2 = jobInfo2.jobId
      val expectedJobInfo2 = jobInfo2

      save = Await.result(dao.saveJob(jobInfo2), timeout)
      save should equal (true)

      // Destroy and bring up the DB again
      dao = null
      dao = new MetaDataSqlDAO(config)

      val jobs = Await.result(dao.getJobs(2), timeout)
      val jobIds = jobs map { _.jobId }

      jobIds should equal (Seq(jobId2, jobId))
      jobs should equal (Seq(expectedJobInfo2, expectedJobInfo))
    }

    it("list limited amount of jobs") {
      var save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
            jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      save = Await.result(dao.saveJob(jobInfoNoEndNoErr), timeout)
      save should equal (true)

      val jobInfo2 = genJobInfo(jarInfo, true, JobStatus.Running)
      val jobId2 = jobInfo2.jobId
      val expectedJobInfo2 = jobInfo2

      save = Await.result(dao.saveJob(jobInfo2), timeout)
      save should equal (true)

      val jobs: Seq[JobInfo] = Await.result(dao.getJobs(1), timeout)
      jobs.size should equal (1)
    }

    it("saving a JobInfo with the same jobId should update the JOBS table") {
      var save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
            jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)

      val expectedNoEndNoErr = jobInfoNoEndNoErr
      val expectedSomeEndNoErr = jobInfoSomeEndNoErr
      val expectedSomeEndSomeErr = jobInfoSomeEndSomeErr
      val exJobId = jobInfoNoEndNoErr.jobId

      val info = genJarInfo(true, false)
      info.uploadTime should equal (jarInfo.uploadTime)

      // Cannot compare JobInfos directly if error is a Some(Throwable) because
      // Throwable uses referential equality
      save = Await.result(dao.saveJob(expectedNoEndNoErr), timeout)
      save should equal (true)
      val jobs2 = Await.result(dao.getJobs(1), timeout)
      jobs2.size should equal (1)
      jobs2.last.endTime should equal (None)
      jobs2.last.error shouldBe None

      save = Await.result(dao.saveJob(jobInfoSomeEndNoErr), timeout)
      save should equal (true)
      val jobs3 = Await.result(dao.getJobs(1), timeout)
      jobs3.size should equal (1)
      jobs3.last.error.isDefined should equal (false)
      jobs3.last should equal (expectedSomeEndNoErr)

      // Cannot compare JobInfos directly if error is a Some(Throwable) because
      // Throwable uses referential equality
      save = Await.result(dao.saveJob(jobInfoSomeEndSomeErr), timeout)
      save should equal (true)
      val jobs4 = Await.result(dao.getJobs(1), timeout)
      jobs4.size should equal (1)
      jobs4.last.endTime should equal (expectedSomeEndSomeErr.endTime)
      jobs4.last.error shouldBe defined
      jobs4.last.error.get.message should equal (throwable.getMessage)
      jobs4.last.error.get.errorClass should equal (throwable.getClass.getName)
      jobs4.last.error.get.stackTrace should not be empty
    }

    it("should return empty list if no job exists using binary") {
      Await.result(dao.getJobsByBinaryName("I-don't-exist"), timeout) should equal(Seq.empty)
    }

    it("should get jobs by binary name") {
      val isSaved = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
        jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      isSaved should equal(true)

      Await.result(dao.saveJob(jobInfoNoEndNoErr), timeout) should equal(true)

      Await.result(dao.getJobsByBinaryName(jarInfo.appName), timeout).head should equal(jobInfoNoEndNoErr)
    }

    it("should get jobs by binary name and status") {
      // setup
      val isSaved = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
        jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      isSaved should equal(true)

      val runningJob = jobInfoNoEndNoErr
      val restartingJob = jobInfoNoEndNoErr.copy(jobId = "restarting", state = JobStatus.Restarting)
      val finishedJob = jobInfoNoEndNoErr.copy(jobId = "finished", state = JobStatus.Finished)

      Await.result(dao.saveJob(runningJob), timeout) should equal(true)
      Await.result(dao.saveJob(restartingJob), timeout) should equal(true)
      Await.result(dao.saveJob(finishedJob), timeout) should equal(true)

      // test
      val getRunningJobResult = Await.result(
        dao.getJobsByBinaryName(jarInfo.appName, Some(Seq(JobStatus.Running))), timeout)

      // assert
      getRunningJobResult.length should be(1)
      getRunningJobResult.head should equal(runningJob)

      // test-2
      val getRunningAndFinishedJobsResult = Await.result(
        dao.getJobsByBinaryName(jarInfo.appName, Some(Seq(JobStatus.Running, JobStatus.Finished))), timeout)

      // assert-2
      getRunningAndFinishedJobsResult.length should be(2)
      getRunningAndFinishedJobsResult should contain allOf(runningJob, finishedJob)
    }

    it("should store callbackUrl") {
      var save = Await.result(dao.saveBinary(jarInfo.appName, BinaryType.Jar,
        jarInfo.uploadTime, BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)
      save should equal (true)
      val id = jobInfoSomeCallback.jobId
      save = Await.result(dao.saveJob(jobInfoSomeCallback), timeout)
      save should equal (true)
      val job = Await.result(dao.getJob(id), timeout).get

      job should equal (jobInfoSomeCallback)
    }
  }
}