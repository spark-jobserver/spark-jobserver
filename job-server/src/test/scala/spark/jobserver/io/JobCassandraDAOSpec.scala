package spark.jobserver.io

import java.io.File
import java.util.UUID

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.utils.UUIDs
import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.TestJarFinder
import scala.concurrent.duration._
import scala.concurrent.Await

class JobCassandraDAOSpec extends TestJarFinder with FunSpecLike with Matchers with BeforeAndAfter
  with BeforeAndAfterAll {

  private val config = ConfigFactory.load("local.test.jobsqldao.conf")

  var dao: JobCassandraDAO = _

  // *** TEST DATA ***
  val time: DateTime = new DateTime()
  val timeout = 5 seconds
  val throwable: Throwable = new Throwable("test-error")
  // jar test data
  val jarInfo: BinaryInfo = genJarInfo(false, false)
  val jarBytes: Array[Byte] = Files.toByteArray(testJar)
  var jarFile: File = new File(
    config.getString("spark.jobserver.sqldao.rootdir"),
    jarInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".jar"
  )

  // jobInfo test data; order is important
  val jobInfoNoEndNoErr:JobInfo = genJobInfo(jarInfo, false, false, false)
  val expectedJobInfo = jobInfoNoEndNoErr
  val jobInfoNoEndSomeErr: JobInfo = genJobInfo(jarInfo, false, true, false)
  val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, true, false, false)
  val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, true, true, false)

  // job config test data
  val jobId: String = jobInfoNoEndNoErr.jobId
  val jobConfig: Config = ConfigFactory.parseString("{marco=pollo}")
  val expectedConfig: Config = ConfigFactory.empty()
    .withValue("marco", ConfigValueFactory.fromAnyRef("pollo"))

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

    def genTestJobInfo(jarInfo: BinaryInfo, hasEndTime: Boolean, hasError: Boolean, isNew:Boolean):JobInfo ={
      count = count + (if (isNew) 1 else 0)

      val id: String = UUIDs.random().toString
      val contextName: String = "test-context"
      val classPath: String = "test-classpath"
      val startTime: DateTime = new DateTime()

      val noEndTime: Option[DateTime] = None
      val someEndTime: Option[DateTime] = Some(startTime.plusSeconds(5)) // Any DateTime Option is fine
      val noError: Option[Throwable] = None
      val someError: Option[Throwable] = Some(throwable)

      val endTime: Option[DateTime] = if (hasEndTime) someEndTime else noEndTime
      val error: Option[Throwable] = if (hasError) someError else noError

      Thread.sleep(2) // hack to guarantee order
      JobInfo(id, contextName, jarInfo, classPath, startTime, endTime, error)
    }

    genTestJobInfo _
  }

  def genJarInfo: (Boolean, Boolean) => BinaryInfo = genJarInfoClosure
  def genJobInfo: (BinaryInfo, Boolean, Boolean, Boolean) => JobInfo = genJobInfoClosure
  //**********************************
  override def beforeAll() {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
    val session = Cluster.builder.addContactPoint("localhost").withPort(9142).build().connect()
    session.execute(
      "CREATE KEYSPACE spark_jobserver " +
      "WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};")
  }

  before {
    dao = new JobCassandraDAO(config)

    jarFile.delete()
  }

  describe("save and get the jars") {
    it("should be able to save one jar and get it back") {
      // check the pre-condition
      jarFile.exists() should equal (false)

      // save
      dao.saveBinary(jarInfo.appName, jarInfo.binaryType, jarInfo.uploadTime, jarBytes)

      // read it back
      val apps = Await.result(dao.getApps, timeout)

      // test
      jarFile.exists() should equal (true)
      apps.keySet should equal (Set(jarInfo.appName))
      apps(jarInfo.appName)._2 should equal (jarInfo.uploadTime)
      apps(jarInfo.appName)._1 should be (BinaryType.Jar)
    }

    it("should be able to retrieve the jar file") {
      // check the pre-condition
      jarFile.exists() should equal (false)

      // retrieve the jar file
      val jarFilePath: String = dao.retrieveBinaryFile(jarInfo.appName, jarInfo.binaryType, jarInfo.uploadTime)

      // test
      jarFile.exists() should equal (true)
      jarFilePath should equal (jarFile.getAbsolutePath)
      val retrieved = new File(jarFilePath)
      jarFile.length() should equal (retrieved.length())
    }

    it("should retrieve the jar binary content for remote job manager") {
      // chack the pre-condition
      jarFile.exists() should equal (false)

      // retrieve the jar content
      val jarBinaryContent: Array[Byte] = dao.getBinaryContent(jarInfo.appName, jarInfo.binaryType, jarInfo.uploadTime)

      // test
      jarFile.exists() should equal (true)
      jarBinaryContent.length should equal (jarBytes.length)
    }
  }

  describe("saveJobConfig() and getJobConfigs() tests") {
    it("should provide an empty map on getJobConfigs() for an empty CONFIGS table") {
      val configs = Await.result(dao.getJobConfigs, timeout)
      (Map.empty[String, Config]) should equal (configs)
    }

    it("should save and get the same config") {
      // save job config
      dao.saveJobConfig(jobId, jobConfig)

      // get all configs
      val configs = Await.result(dao.getJobConfigs, timeout)

      // test
      configs.keySet should equal (Set(jobId))
      configs(jobId) should equal (expectedConfig)
    }

    it("should be able to get previously saved config") {
      // config saved in prior test

      // get job configs
      val configs = Await.result(dao.getJobConfigs, timeout)

      // test
      configs.keySet should equal (Set(jobId))
      configs(jobId) should equal (expectedConfig)
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
      dao = new JobCassandraDAO(config)

      // Get all configs
      val configs = Await.result(dao.getJobConfigs, timeout)

      // test
      configs.keySet should equal (Set(jobId, jobId2))
      configs.values.toSeq should contain (expectedConfig)
      configs.values.toSeq should contain (expectedConfig2)
    }
  }

  describe("Basic saveJobInfo() and getJobInfos() tests") {
    it("should provide an empty Seq on getJobInfos() for an empty JOBS table") {
      val jobs = Await.result(dao.getJobInfos(1), timeout)
      (Seq.empty[JobInfo]) should equal (jobs)
    }

    it("should save a new JobInfo and get the same JobInfo") {
      // save JobInfo
      dao.saveJobInfo(jobInfoNoEndNoErr)

      // get some JobInfos
      val jobs = Await.result(dao.getJobInfos(10), timeout)

      // test
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
      dao = new JobCassandraDAO(config)

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
      jobs2.last.error.isDefined should equal (true)
      intercept[Throwable] { jobs2.last.error.map(throw _) }
      jobs2.last.error.get.getMessage should equal (throwable.getMessage)

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
      jobs4.last.error.isDefined should equal (true)
      intercept[Throwable] { jobs4.last.error.map(throw _) }
      jobs4.last.error.get.getMessage should equal (throwable.getMessage)
    }
    it("retrieve by status equals running should be no end and no error") {
      //save some job insure exist one running job
      val dt1 = DateTime.now()
      val dt2 = Some(DateTime.now())
      val someError = Some(new Throwable("test-error"))
      val finishedJob: JobInfo =
        JobInfo(UUID.randomUUID.toString, "test", jarInfo, "test-class", dt1, dt2, None)
      val errorJob: JobInfo =
        JobInfo(UUID.randomUUID.toString, "test", jarInfo, "test-class", dt1, dt2, someError)
      val runningJob: JobInfo =
        JobInfo(UUID.randomUUID.toString, "test", jarInfo, "test-class", dt1, None, None)
      dao.saveJobInfo(finishedJob)
      dao.saveJobInfo(runningJob)
      dao.saveJobInfo(errorJob)

      //retrieve by status equals RUNNING
      val retrieved = Await.result(dao.getJobInfos(3, Some(JobStatus.Running)), timeout).head

      //test
      retrieved.endTime.isDefined should equal (false)
      retrieved.error.isDefined should equal (false)
    }
    it("retrieve by status equals finished should be some end and no error") {

      //retrieve by status equals FINISHED
      val retrieved = Await.result(dao.getJobInfos(3, Some(JobStatus.Finished)), timeout).head

      //test
      retrieved.endTime.isDefined should equal (true)
      retrieved.error.isDefined should equal (false)
    }

    it("retrieve by status equals error should be some error") {
      //retrieve by status equals ERROR
      val retrieved = Await.result(dao.getJobInfos(3, Some(JobStatus.Error)), timeout).head

      //test
      retrieved.error.isDefined should equal (true)
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
