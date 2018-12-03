package spark.jobserver.io

import scala.concurrent.Await
import scala.concurrent.duration._

import java.io.File

import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
import spark.jobserver.TestJarFinder
import java.util.UUID

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

  describe("Migration Tests") {
    it("should return empty array if hash doesn't matches") {
      Await.result(dao.getBinaryBytes(""), timeout) should be(Array())
      Await.result(dao.getBinaryBytes("a"), timeout) should be(Array())
    }

    it("should return all hashes") {
      // Cleanup
      val apps = Await.result(dao.getApps, timeout)
      apps.map(a => dao.deleteBinary(a._1))

      Await.result(dao.getAllHashes(), timeout) should be(Seq())

      dao.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)
      dao.saveBinary("test", BinaryType.Jar, DateTime.now(), Array(1, 2))

      val hashes = Await.result(dao.getAllHashes(), timeout)
      hashes(0) should be(BinaryDAO.calculateBinaryHashString(jarBytes))
      hashes(1) should be(BinaryDAO.calculateBinaryHashString(Array(1, 2)))
    }

    it("should return valid binary if it matches") {
      dao.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)

      val fetchedBytes = Await.result(
        dao.getBinaryBytes(BinaryDAO.calculateBinaryHashString(jarBytes)), timeout)

      fetchedBytes should be(jarBytes)
    }

    it("should return hash for app if the same hash is not used in any other app") {
      Await.result(dao.getHashForApp("dummy"), timeout) should be(Seq())
      Await.result(dao.getHashForApp("dummy1"), timeout) should be(Seq())

      dao.saveBinary("dummy", BinaryType.Jar, jarInfo.uploadTime, Array(3, 4))
      dao.saveBinary("dummy1", BinaryType.Jar, DateTime.now(), Array(5, 6))

      Await.result(dao.getHashForApp("dummy"), timeout) should
        be(Seq(BinaryDAO.calculateBinaryHashString(Array(3, 4))))

      Await.result(dao.getHashForApp("dummy1"), timeout) should
        be(Seq(BinaryDAO.calculateBinaryHashString(Array(5, 6))))
    }

    it("should not return any hash if multiple binaries have the same hash") {
      val bytes = Array[Byte](7, 8)
      dao.saveBinary("same", BinaryType.Jar, jarInfo.uploadTime, bytes)
      dao.saveBinary("same1", BinaryType.Jar, DateTime.now(), bytes)
      dao.saveBinary("same2", BinaryType.Jar, DateTime.now(), bytes)

      Await.result(dao.getHashForApp("same"), timeout) should be(Seq())

      // Cross verify with H2
      dao.deleteBinary("same")
      Await.result(dao.getBinaryBytes(
        BinaryDAO.calculateBinaryHashString(bytes)), timeout) should be(bytes)

      // Since some app is using it, shouldn't delete from HDFS
      Await.result(dao.getHashForApp("same1"), timeout) should be(Seq())

      dao.deleteBinary("same1")
      Await.result(dao.getBinaryBytes(
        BinaryDAO.calculateBinaryHashString(bytes)), timeout) should be(bytes)

      // Since same2 is the only app using the hash now, it should be deleted from HDFS
      Await.result(dao.getHashForApp("same2"), timeout) should be(
        Seq(BinaryDAO.calculateBinaryHashString(bytes)))

      // Since same2 is the only app using the hash now, it should be deleted from H2
      dao.deleteBinary("same2")
      Await.result(dao.getBinaryBytes(
        BinaryDAO.calculateBinaryHashString(bytes)), timeout) should be(Array.emptyByteArray)
    }

    it("should return multiple hashes if there are multiple apps with same name but different hash") {
      val bytesA = Array[Byte](7, 8)
      val bytesB = Array[Byte](7, 8, 9)
      dao.saveBinary("same", BinaryType.Jar, jarInfo.uploadTime, bytesA)
      dao.saveBinary("same", BinaryType.Jar, DateTime.now(), bytesB)

      Await.result(dao.getHashForApp("same"), timeout).sorted should be(
        Seq(BinaryDAO.calculateBinaryHashString(bytesA),
        BinaryDAO.calculateBinaryHashString(bytesB)
        ).sorted)

      // Cross verify with H2
      // Since no other app is using the binary. H2 will also cleanup the Binaries_content table
      // Deletion should happen in HDFS
      dao.deleteBinary("same")
      Await.result(dao.getBinaryBytes(
        BinaryDAO.calculateBinaryHashString(bytesA)), timeout) should be(Array.emptyByteArray)
      Await.result(dao.getBinaryBytes(
        BinaryDAO.calculateBinaryHashString(bytesB)), timeout) should be(Array.emptyByteArray)
    }

    it("should not return any hash even if only 1 hash for an app is being used") {
      val bytesA = Array[Byte](7, 8)
      val bytesB = Array[Byte](7, 8, 9)
      val diffUploadTime = DateTime.now()
      dao.saveBinary("same", BinaryType.Jar, jarInfo.uploadTime, bytesA)
      dao.saveBinary("same", BinaryType.Jar, DateTime.now(), bytesB)
      dao.saveBinary("different", BinaryType.Jar, diffUploadTime, bytesB)

      Await.result(dao.getHashForApp("same"), timeout) should be(Seq())

      // Cross verify with H2
      // H2 should still have both binaries in Binaries_content table
      // So, nothing should be deleted from HDFS
      dao.deleteBinary("same")
      Await.result(dao.getBinaryBytes(
        BinaryDAO.calculateBinaryHashString(bytesB)), timeout) should be(bytesB)
      Await.result(dao.getBinaryBytes(
        BinaryDAO.calculateBinaryHashString(bytesA)), timeout) should be(bytesA)
    }

    it("should return hash if no other app has the same hash") {
      val binary = Array[Byte](9, 10)
      dao.saveBinary("only-one", BinaryType.Jar, jarInfo.uploadTime, binary)
      Await.result(dao.getHashForApp("only-one"), timeout) should be(
        Seq(BinaryDAO.calculateBinaryHashString(binary)))
    }

    it("should return empty sequence if no app name matches") {
      Await.result(dao.getHashForApp("doesnt-exist"), timeout) should be(Seq())
    }
  }

}

class JobSqlDAODBCPSpec extends JobSqlDAOSpec {
  override def config: Config = ConfigFactory.load("local.test.jobsqldao_dbcp.conf")
}
