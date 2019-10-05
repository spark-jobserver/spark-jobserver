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
import org.scalatest._
import spark.jobserver.util.Utils

class JobCassandraDAOSpec extends TestJarFinder with FunSpecLike with Matchers with BeforeAndAfter
  with BeforeAndAfterAll {

  private val config = ConfigFactory.load("local.test.jobsqldao.conf")

  private var dao: JobCassandraDAO = _

  // *** TEST DATA ***
  private val time: DateTime = new DateTime()
  private val timeout = 5 seconds
  private val throwable: Throwable = new Throwable("test-error")
  // jar test data
  private val jarInfo: BinaryInfo = genJarInfo(false, false)
  private val jarBytes: Array[Byte] = Files.toByteArray(testJar)
  private var jarFile: File = new File(
    config.getString("spark.jobserver.sqldao.rootdir"),
    jarInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_HHmmss_SSS") + ".jar"
  )

  // jobInfo test data; order is important
  private val jobInfoNoEndNoErr: JobInfo = genJobInfo(jarInfo, JobStatus.Running)
  private val expectedJobInfo: JobInfo = jobInfoNoEndNoErr
  private val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, JobStatus.Finished)
  private val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, JobStatus.Error)

  // job config test data
  private val jobId: String = jobInfoNoEndNoErr.jobId
  private val jobConfig: Config = ConfigFactory.parseString("{marco=pollo}")
  private val expectedConfig: Config = ConfigFactory.empty()
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

  case class GenJobInfoClosure() {
    def apply(jarInfo: BinaryInfo, state: String, contextName: String = "test-context",
        contextId: String = UUIDs.random().toString): JobInfo = {

      val id: String = UUIDs.random().toString
      val classPath: String = "test-classpath"
      val startTime: DateTime = new DateTime()

      val noEndTime: Option[DateTime] = None
      val someEndTime: Option[DateTime] = Some(startTime.plusSeconds(5)) // Any DateTime Option is fine
      val someError = Some(ErrorData(throwable))

      val endTimeAndError = state match {
        case JobStatus.Started | JobStatus.Running => (None, None)
        case JobStatus.Finished => (someEndTime, None)
        case JobStatus.Error | JobStatus.Killed => (someEndTime, someError)
      }

      Thread.sleep(2) // hack to guarantee order
      JobInfo(id, contextId, contextName, jarInfo, classPath, state, startTime,
          endTimeAndError._1, endTimeAndError._2)
    }

  }

  def genJarInfo: (Boolean, Boolean) => BinaryInfo = genJarInfoClosure
  lazy val genJobInfo = GenJobInfoClosure()
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
      val jarFilePath: String = dao.getBinaryFilePath(jarInfo.appName, jarInfo.binaryType, jarInfo.uploadTime)

      // test
      jarFile.exists() should equal (true)
      jarFilePath should equal (jarFile.getAbsolutePath)
      val retrieved = new File(jarFilePath)
      jarFile.length() should equal (retrieved.length())
      Files.toByteArray(jarFile) should equal(Files.toByteArray(retrieved))
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

      val config = Utils.retry(3, retryDelayInMs = 1000)(Await.result(dao.getJobConfig(jobId), timeout).get)

      // test
      config should equal (expectedConfig)
    }

    it("should be able to get previously saved config") {
      // config saved in prior test

      // get job configs
      val config = Await.result(dao.getJobConfig(jobId), timeout).get

      // test
      config should equal (expectedConfig)
    }

    it("Save a new config, bring down DB, bring up DB, should get configs from DB") {
      val jobId2: String = genJobInfo(genJarInfo(false, false), JobStatus.Running).jobId
      val jobConfig2: Config = ConfigFactory.parseString("{merry=xmas}")
      val expectedConfig2 = ConfigFactory.empty().withValue("merry", ConfigValueFactory.fromAnyRef("xmas"))
      // config previously saved

      // save new job config
      dao.saveJobConfig(jobId2, jobConfig2)

      // Destroy and bring up the DB again
      dao = null
      dao = new JobCassandraDAO(config)

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
      val jobs = Await.result(dao.getJobInfos(1), timeout)
      Seq.empty[JobInfo] should equal (jobs)
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
      val jobInfo2 = genJobInfo(jarInfo, JobStatus.Running)
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
      val expectedNoEndNoErr = jobInfoNoEndNoErr
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
      dao.saveJobInfo(expectedNoEndNoErr)
      val jobs2 = Await.result(dao.getJobInfos(2), timeout)
      jobs2.size should equal (2)
      jobs2.last.endTime should equal (None)
      jobs2.last.error should equal (None)

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
      val finishedJob: JobInfo = JobInfo(UUID.randomUUID.toString, UUID.randomUUID.toString,
          "test", jarInfo, "test-class", JobStatus.Finished, dt1, dt2, None)
      val errorJob: JobInfo = JobInfo(UUID.randomUUID.toString, UUID.randomUUID.toString,
          "test", jarInfo, "test-class", JobStatus.Error, dt1, dt2, someError)
      val runningJob: JobInfo = JobInfo(UUID.randomUUID.toString, UUID.randomUUID.toString,
          "test", jarInfo, "test-class", JobStatus.Running, dt1, None, None)
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

    it("should retrieve jobs by context id") {
      val jobInfo = genJobInfo(jarInfo, JobStatus.Running, "context")
      dao.saveJobInfo(jobInfo)

      val results = Await.result(dao.getJobInfosByContextId(jobInfo.contextId), timeout)

      results should have size 1
      results.head.jobId shouldBe jobInfo.jobId
    }

    it("should retrieve multiple jobs if they have the same context id") {
      val contextId = UUIDs.random().toString
      val finishedJob = genJobInfo(jarInfo, JobStatus.Finished, "context", contextId)
      val jobWithDifferentContextId = genJobInfo(jarInfo, JobStatus.Error)
      val runningJob = genJobInfo(jarInfo, JobStatus.Running, "context", contextId)
      val finishedJob2 = genJobInfo(jarInfo, JobStatus.Finished, "context", contextId)
      dao.saveJobInfo(finishedJob)
      dao.saveJobInfo(jobWithDifferentContextId)
      dao.saveJobInfo(runningJob)
      dao.saveJobInfo(finishedJob2)

      val results = Await.result(dao.getJobInfosByContextId(contextId), timeout)

      results should have size 3
      results.filter(_.contextId == contextId).size shouldBe 3
      results.map(_.jobId) should contain allOf(finishedJob.jobId, runningJob.jobId, finishedJob2.jobId)
    }

    it("retrieve finished jobs by context id") {
      val contextId = UUIDs.random().toString
      val finishedJob = genJobInfo(jarInfo, JobStatus.Finished, "context", contextId)
      val runningJob = genJobInfo(jarInfo, JobStatus.Running, "context", contextId)
      dao.saveJobInfo(finishedJob)
      dao.saveJobInfo(runningJob)

      val results = Await.result(
        dao.getJobInfosByContextId(contextId, Some(Seq(JobStatus.Finished))), timeout)
      results should have size 1
      results.head.jobId shouldBe finishedJob.jobId //Jobid is unique
    }

    it("retrieve finished and running jobs by context id") {
      val contextId = UUIDs.random().toString
      val finishedJob = genJobInfo(jarInfo, JobStatus.Finished, "context", contextId)
      val runningJob = genJobInfo(jarInfo, JobStatus.Running, "context", contextId)
      val errorJob = genJobInfo(jarInfo, JobStatus.Error, "context", contextId)
      dao.saveJobInfo(finishedJob)
      dao.saveJobInfo(runningJob)
      dao.saveJobInfo(errorJob)

      val results = Await.result(dao.getJobInfosByContextId(
          contextId, Some(Seq(JobStatus.Finished, JobStatus.Running))), timeout)
      results should have size 2
      results should contain allElementsOf(List(runningJob, finishedJob))
    }

    it("should retrieve nothing if no job with the specified status exist") {
      val contextId = UUID.randomUUID().toString()
      val runningJob = genJobInfo(jarInfo, JobStatus.Running, "context", contextId)
      dao.saveJobInfo(runningJob)

      val results = Await.result(
          dao.getJobInfosByContextId(contextId, Some(Seq(JobStatus.Killed, JobStatus.Finished))), timeout)

      results shouldBe empty
    }

    it("should clean jobs for given context id") {
      val jobInfo = genJobInfo(jarInfo, JobStatus.Running, "context")
      dao.saveJobInfo(jobInfo)

      Await.result(dao.cleanRunningJobInfosForContext(jobInfo.contextId, DateTime.now()), timeout)

      val updatedJobInfo = Await.result(dao.getJobInfo(jobInfo.jobId), timeout)
      updatedJobInfo shouldBe defined
      updatedJobInfo.get.state shouldBe JobStatus.Error
      updatedJobInfo.get.endTime shouldBe defined
      updatedJobInfo.get.error shouldBe defined
    }
  }

  describe("Context table tests") {
    it("should return None if context id does not exist in Contexts table") {
      val context = Await.result(dao.getContextInfo(UUIDs.random().toString), timeout)
      context.isDefined should equal (false)
    }

    it("should save a new ContextInfo and get the same ContextInfo") {
      val uuid = UUIDs.random().toString
      val contextInfo = ContextInfo(
        uuid, "test-context", "", None, DateTime.now(), None, ContextStatus.Started, None)
      dao.saveContextInfo(contextInfo)

      val context = Await.result(dao.getContextInfo(uuid), timeout)
      context.get should equal (contextInfo)
    }

    it("should store context with actor address and endtime") {
      val uuid = UUIDs.random().toString
      val contextInfo = ContextInfo(
        uuid, "test-context1", "", Some("akka.tcp://"), DateTime.now(),
        Some(DateTime.now().plusDays(1)), ContextStatus.Started, None
      )

      dao.saveContextInfo(contextInfo)

      val context = Await.result(dao.getContextInfo(uuid), timeout)
      context.get should be (contextInfo)
    }

    it("should update the context if id is the same for two saveContextInfo requests") {
      val uuid = UUIDs.random().toString
      val contextInfo = ContextInfo(
        uuid, _: String, "", None, DateTime.now(), None, ContextStatus.Started, None)
      lazy val contextInfo2 = contextInfo("test-context2")

      dao.saveContextInfo(contextInfo("test-context3"))
      dao.saveContextInfo(contextInfo2)

      val context = Await.result(dao.getContextInfo(uuid), timeout)
      context.get should be (contextInfo2)
    }

    it("should be able to save context with error") {
      val uuid = UUIDs.random().toString
      val contextInfo = ContextInfo(
        uuid, "test-context4", "", None, DateTime.now(), None,
        ContextStatus.Started, Some(new Exception("test"))
      )

      dao.saveContextInfo(contextInfo)

      val context = Await.result(dao.getContextInfo(uuid), timeout)
      context.get.error.get.getMessage should be ("test")
    }

    it("should get the latest contextInfo by name") {
      val uuid = UUIDs.random().toString
      val contextInfo = ContextInfo(
        uuid, "test-context5", "", None, DateTime.now(), None, ContextStatus.Started, None)

      dao.saveContextInfo(contextInfo)

      val context = Await.result(dao.getContextInfoByName("test-context5"), timeout)
      context.get should be (contextInfo)
    }

    it("should get latest context if multiple contexts exist with the same name") {
      val contextInfo = ContextInfo(
        UUIDs.random().toString, "test-context6", "", None,
        DateTime.now(), None, ContextStatus.Finished, None
      )
      val contextInfo2 = ContextInfo(
        UUIDs.random().toString, "test-context6", "", None,
        DateTime.now().plusMillis(1), None, ContextStatus.Started, None
      )

      dao.saveContextInfo(contextInfo)
      dao.saveContextInfo(contextInfo2)

      val context = Await.result(dao.getContextInfoByName("test-context6"), timeout)
      context.get should be (contextInfo2)
    }

    it("should get latest contexts by status and limit") {
      val contextInfo = ContextInfo(_: String, _: String, "", None, _: DateTime, None, _: String, None)
      val contextInfo1 = contextInfo(
        UUIDs.random().toString, "test-context7", DateTime.now(), ContextStatus.Started)
      val contextInfo2 = contextInfo(
        UUIDs.random().toString, "test-context8", DateTime.now(), ContextStatus.Running)
      val contextInfo3 = contextInfo(
        UUIDs.random().toString, "test-context9", DateTime.now(), ContextStatus.Started)
      val contextInfo4 = contextInfo(
        UUIDs.random().toString, "test-context10",DateTime.now(), ContextStatus.Started)
      val contextInfo5 = contextInfo(
        UUIDs.random().toString, "test-context11",DateTime.now(), ContextStatus.Restarting)

      dao.saveContextInfo(contextInfo1)
      dao.saveContextInfo(contextInfo2)
      dao.saveContextInfo(contextInfo3)
      dao.saveContextInfo(contextInfo4)
      dao.saveContextInfo(contextInfo5)

      def Desc[T : Ordering]: Ordering[T] = implicitly[Ordering[T]].reverse
      var orderedExpectedList = Seq(contextInfo4, contextInfo3, contextInfo1)
                                    .sortBy(_.id)
                                    .sortBy(_.startTime.getMillis)(Desc)

      var contexts = Await.result(dao.getContextInfos(Some(3), Some(Seq(ContextStatus.Started))), timeout)
      contexts should be (orderedExpectedList)

      orderedExpectedList = Seq(contextInfo5, contextInfo2)
          .sortBy(_.startTime.getMillis)(Desc)

      contexts = Await.result(
          dao.getContextInfos(Some(2), Some(Seq(ContextStatus.Running, ContextStatus.Restarting))), timeout)
      contexts should be (orderedExpectedList)
    }

    it("should get latest contexts by status only") {
      val contextInfo = ContextInfo(_: String, _: String, "", None, _: DateTime, None, _: String, None)
      val contextInfo1 = contextInfo(
        UUIDs.random().toString, "test-context11", DateTime.now(), ContextStatus.Started)
      val contextInfo2 = contextInfo(
        UUIDs.random().toString, "test-context12", DateTime.now(), ContextStatus.Running)

      dao.saveContextInfo(contextInfo1)
      dao.saveContextInfo(contextInfo2)
      val contexts = Await.result(dao.getContextInfos(None, Some(Seq(ContextStatus.Started))), timeout)
      contexts.head should be (contextInfo1)
    }

    it("should get unsupported exception if all contexts are listed") {
      assertThrows[UnsupportedOperationException] {
        Await.result(dao.getContextInfos(None, None), timeout)
      }
    }

    it("should get unsupported exception if only limit is provided") {
      assertThrows[UnsupportedOperationException] {
        Await.result(dao.getContextInfos(Some(5), None), timeout)
      }
    }
  }

  describe("delete binaries") {
    it("should be able to delete jar file") {
      val existing = Await.result(dao.getApps, timeout)
      existing.keys should contain (jarInfo.appName)
      dao.deleteBinary(jarInfo.appName)

      val apps = Await.result(dao.getApps, timeout)
      apps.keys should not contain jarInfo.appName
    }
  }
}
