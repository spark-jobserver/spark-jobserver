package spark.jobserver

import java.io.File

import akka.actor.{ActorRef, PoisonPill, Props}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.mockito.{InjectMocks, Mockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.mockito.MockitoSugar
import spark.jobserver.ZookeeperMigrationActor.{ImportJobsForContext, ProcessJobInfo}
import spark.jobserver.io.JobDAOActorSpec.DummyDao
import spark.jobserver.io._
import spark.jobserver.io.zookeeper.{MetaDataZookeeperDAO, ZookeeperUtils}
import spark.jobserver.util.{CuratorTestCluster, Utils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source

object MigrationActorSpec extends JobSpecConfig

class ZookeeperMigrationActorSpec extends JobSpecBase(MigrationActorSpec.getNewSystem) with BeforeAndAfter
  with MockitoSugar{
  private var migrationActor: ActorRef = _
  var daoActorProb: TestProbe = _
  private val testServer = new CuratorTestCluster()
  private val timeout = 60 seconds

  private val testDir = "db/jobserver-test"
  private val migration_file_path = "/tmp/migrationzk"
  private val migration_file_name = "sync_db_to_zookeeper"

  def config: Config = ConfigFactory.parseString(
    s"""
       |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}",
       |spark.jobserver.zookeeperdao.dir = $testDir,
       |spark.jobserver.combineddao.rootdir = $migration_file_path,
       |spark.jobserver.combineddao.binarydao.class = spark.jobserver.io.DummyBinaryDAO,
       |spark.jobserver.combineddao.metadatadao.class = spark.jobserver.io.DummyMetaDataDAO
      """.stripMargin)

  private val zkDao = new MetaDataZookeeperDAO(config)
  private val zkUtils = new ZookeeperUtils(testServer.getConnectString, testDir, 1)

  private val binary = "Some test bin".toCharArray.map(_.toByte)
  private val testInfo = BinaryInfo("test", BinaryType.Jar, DateTime.now(),
    Some(BinaryDAO.calculateBinaryHashString(binary)))
  private val testContextInfo = ContextInfo("contextId", "name", "{}", None,
    DateTime.now(), None, ContextStatus.Started, None)
  private val testJobInfo = JobInfo("jobId", "contextId", "name", testInfo,
    "", JobStatus.Running, DateTime.now(), None, None)
  private val testJobConfig = ConfigFactory.parseString("input = foo")


  val binJar = BinaryInfo("binaryWithJar", BinaryType.Jar, new DateTime(),
    Some(BinaryDAO.calculateBinaryHashString("1".getBytes)))

  // Jobs
  val someJob1 = JobInfo("1", "someContextId", "someContextName", binJar,
    "someClassPath", "someState", new DateTime(), Some(new DateTime()),
    Some(ErrorData("someMessage", "someError", "someTrace")))
  val someJob2 = JobInfo("2", "someContextId", "someOtherContextName", binJar,
    "someClassPath", "someState", new DateTime().plusHours(1), None, None)
  val someJob3 = JobInfo("3", "someOtherContextId", "thirdContextName", binJar,
    "someClassPath", "someState", new DateTime().minusHours(1), None, None)
  val someJob4 = JobInfo("4", "someOtherContextId", "thirdContextName", binJar,
    "someClassPath", "anotherState", new DateTime().minusHours(1), None, None)

  val config1 = ConfigFactory.parseString("{key : value}")

  val sampleContextId = "contextName"

  before {
    testServer.createBaseDir(testDir)
    daoActorProb = TestProbe()
    migrationActor = system.actorOf(ZookeeperMigrationActor.props(config, DummyDao))
  }

  after {
    migrationActor ! PoisonPill
    Utils.usingResource(zkUtils.getClient) {
      client => zkUtils.delete(client, "/")
    }
    FileUtils.deleteQuietly(new File(migration_file_path, migration_file_name))
  }

  describe("Test sync H2 database requests") {
    it("should create file with all binaries and contexts keys") {
      val daoMock = Mockito.spy(DummyDao)
      Mockito.when(daoMock.getAllContextsIds).thenReturn(Future.successful(Seq("id1", "id2", "id5")))
      Mockito.when(daoMock.getAllJobIdsToSync).thenReturn(Future.successful(Seq("jobId1", "jobId2")))

      migrationActor = system.actorOf(ZookeeperMigrationActor.props(config, daoMock))

      migrationActor ! ZookeeperMigrationActor.StartDataSyncFromH2

      Thread.sleep(3000)

     val syncFile = Source.fromFile(s"$migration_file_path/$migration_file_name")
     syncFile.getLines().toList should be(
       List("current_index = 1", "total_binary_keys = 2", "total_context_keys = 3", "total_job_keys = 2",
         "total_keys = 7", "1 = binary:app1", "2 = binary:app2", "3 = context:id1", "4 = context:id2",
         "5 = context:id5", "6 = jobs:jobId1", "7 = jobs:jobId2")
     )
      syncFile.close() // UsingResource wrapper didn't recognize type as closable
    }

    it("should not create a file if context keys are not available") {
      val daoMock = Mockito.spy(DummyDao)
      Mockito.when(daoMock.getAllContextsIds).thenReturn(Future.failed(new Exception))
      migrationActor = system.actorOf(ZookeeperMigrationActor.props(config, daoMock))
      migrationActor ! ZookeeperMigrationActor.StartDataSyncFromH2

      Thread.sleep(1000)

      new File(migration_file_path, migration_file_name).exists() should be (false)
    }

    it("should save job info and config") {
      Await.result(zkDao.saveBinary(binJar.appName, binJar.binaryType,
        binJar.uploadTime, binJar.binaryStorageId.get), timeout) should equal (true)

      val daoMock = mock[CombinedDAO]
      Mockito.when(daoMock.getJobInfo(someJob1.jobId)).thenReturn(
        Future.successful(Some(someJob1))
      )
      Mockito.when(daoMock.getJobConfig(someJob1.jobId)).thenReturn(
        Future.successful(Some(config1))
      )
      migrationActor = system.actorOf(ZookeeperMigrationActor.props(config, daoMock))

      migrationActor ! ProcessJobInfo(someJob1.jobId)

      Thread.sleep(2000)

      Await.result(zkDao.getJob(someJob1.jobId), timeout).get should equal (someJob1)
      Await.result(zkDao.getJobConfig(someJob1.jobId), timeout).get should equal (config1)

    }

    it("should not save config if job save failed") {
      Await.result(zkDao.saveBinary(binJar.appName, binJar.binaryType,
        binJar.uploadTime, binJar.binaryStorageId.get), timeout) should equal (true)

      val daoMock = mock[CombinedDAO]
      Mockito.when(daoMock.getJobInfo(someJob1.jobId)).thenReturn(
        Future.failed(new Exception)
      )
      Mockito.when(daoMock.getJobConfig(someJob1.jobId)).thenReturn(
        Future.successful(Some(config1))
      )
      migrationActor = system.actorOf(ZookeeperMigrationActor.props(config, daoMock))

      migrationActor ! ProcessJobInfo(someJob1.jobId)

      Thread.sleep(2000)

      Await.result(zkDao.getJob(someJob1.jobId), timeout) should equal (None)
      Await.result(zkDao.getJobConfig(someJob1.jobId), timeout) should equal (None)
    }

    it("should not process next job id if first job save fails") {
      val daoMock = mock[CombinedDAO]
      Mockito.when(daoMock.getApps).thenReturn(Future.successful(Map.empty[String, (BinaryType, DateTime)]))
      Mockito.when(daoMock.getAllContextsIds).thenReturn(Future.successful(Seq.empty))
      Mockito.when(daoMock.getAllJobIdsToSync).thenReturn(Future.successful(
        Seq(someJob1.jobId, someJob2.jobId, someJob3.jobId)))
      Mockito.when(daoMock.getJobInfo(someJob1.jobId)).thenReturn(Future.successful(Some(someJob1)))
      Mockito.when(daoMock.getJobInfo(someJob2.jobId)).thenReturn(Future.failed(new Exception))
      Mockito.when(daoMock.getJobInfo(someJob3.jobId)).thenReturn(Future.successful(Some(someJob3)))
      Mockito.when(daoMock.getJobConfig(someJob1.jobId)).thenReturn(Future.successful(Some(config1)))
      Mockito.when(daoMock.getJobConfig(someJob2.jobId)).thenReturn(Future.successful(Some(config1)))
      Mockito.when(daoMock.getJobConfig(someJob3.jobId)).thenReturn(Future.successful(Some(config1)))

      migrationActor = system.actorOf(ZookeeperMigrationActor.props(config, daoMock,
        false, 1.seconds, 1.seconds))

      migrationActor ! ZookeeperMigrationActor.StartDataSyncFromH2

      Thread.sleep(2000)

      val syncFile = Source.fromFile(s"$migration_file_path/$migration_file_name")
      val syncFileContent = syncFile.getLines().toList

      syncFileContent(1) should be("total_binary_keys = 0")
      syncFileContent(2) should be("total_context_keys = 0")
      syncFileContent(3) should be("total_job_keys = 3")


      Await.result(zkDao.saveBinary(binJar.appName, binJar.binaryType,
        binJar.uploadTime, binJar.binaryStorageId.get), timeout) should equal (true)

      Thread.sleep(3000) // wait longer to be sure // , syncInterval now 1 sec

      syncFileContent(0) should be("current_index = 2")
      syncFile.close() // UsingResource wrapper didn't recognize type as closable

      Await.result(zkDao.getJob(someJob1.jobId), timeout).get should equal (someJob1)
      Await.result(zkDao.getJob(someJob2.jobId), timeout) should equal (None)
      Await.result(zkDao.getJobConfig(someJob3.jobId), timeout) should equal (None)
    }
  }

  describe("Test live requests") {
    it("should store binary to Zookeeper") {
      migrationActor ! ZookeeperMigrationActor.SaveBinaryInfoInZK(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)

      Thread.sleep(2000)

      Await.result(zkDao.getBinary("test"), timeout).get should equal (testInfo)
    }

    it("should delete binary from Zookeeper") {
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      migrationActor ! ZookeeperMigrationActor.DeleteBinaryInfoFromZK(testInfo.appName)

      Thread.sleep(2000)

      Await.result(zkDao.getBinary("test"), timeout) should equal (None)
    }

    it("should be okay with deleting binary that doesn't exist") {
      migrationActor ! ZookeeperMigrationActor.DeleteBinaryInfoFromZK(testInfo.appName)
      Thread.sleep(2000) // wait for migration worker to save the data
      Await.result(zkDao.getBinary("test"), timeout) should equal (None)
    }

    it("should not give error if same binary is saved again") {
      migrationActor ! ZookeeperMigrationActor.SaveBinaryInfoInZK(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)
      migrationActor ! ZookeeperMigrationActor.SaveBinaryInfoInZK(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)

      Thread.sleep(2000)

      migrationActor ! ZookeeperMigrationActor.SaveBinaryInfoInZK(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)
      Thread.sleep(2000)

      Await.result(zkDao.getBinary("test"), timeout).get should equal (testInfo)
    }

    it("should store context info in Zookeeper") {
      migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(testContextInfo)

      Thread.sleep(2000)

      Await.result(zkDao.getContextByName(testContextInfo.name),
        timeout).get should equal (testContextInfo)
    }

    it("should not raise an error if same context info is saved again") {
      migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(testContextInfo)
      Thread.sleep(2000)

      migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(testContextInfo)
      Thread.sleep(2000)

      Await.result(zkDao.getContextByName(testContextInfo.name),
        timeout).get should equal (testContextInfo)
    }

    it("should store job info in Zookeeper") {
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(testJobInfo)

      Thread.sleep(2000)

      Await.result(zkDao.getJob(testJobInfo.jobId), timeout).get should equal (testJobInfo)
    }

    it("should not raise an error if same job info is saved again") {
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(testJobInfo)
      Thread.sleep(2000)

      val testJobInfoUpdated = testJobInfo.copy(state = "ERROR")
      migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(testJobInfoUpdated)
      Thread.sleep(2000)

      Await.result(zkDao.getJob(testJobInfo.jobId), timeout).get should equal (testJobInfoUpdated)
    }

    it("should store job config in Zookeeper") {
      // binaryInfo and jobConfig need a job to be in DB already
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      Await.result(zkDao.saveJob(testJobInfo), timeout)

      migrationActor ! ZookeeperMigrationActor.SaveJobConfigInZK(testJobInfo.jobId, testJobConfig)
      Thread.sleep(2000)

      Await.result(zkDao.getJobConfig(testJobInfo.jobId), timeout).get should equal (testJobConfig)
    }

    it("should not raise an error if same job config is saved again") {
      // binaryInfo and jobConfig need a job to be in DB already
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      Await.result(zkDao.saveJob(testJobInfo), timeout)

      migrationActor ! ZookeeperMigrationActor.SaveJobConfigInZK(testJobInfo.jobId, testJobConfig)
      Thread.sleep(2000)

      val anotherTestJobConfig = ConfigFactory.parseString("foo = bar")
      migrationActor ! ZookeeperMigrationActor.SaveJobConfigInZK(testJobInfo.jobId, anotherTestJobConfig)
      Thread.sleep(2000)

      Await.result(zkDao.getJobConfig(testJobInfo.jobId), timeout).get should equal (anotherTestJobConfig)
    }
  }
}
