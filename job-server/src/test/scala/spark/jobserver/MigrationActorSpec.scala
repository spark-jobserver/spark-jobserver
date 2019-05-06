package spark.jobserver

import akka.pattern.ask
import akka.actor.ActorRef
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import spark.jobserver.MigrationActor.{SyncDatabases, DatabasesSynced}
import spark.jobserver.io._
import spark.jobserver.io.zookeeper.{MetaDataZookeeperDAO, ZookeeperUtils}
import spark.jobserver.util.{CuratorTestCluster, Utils}

import scala.concurrent.Await
import scala.concurrent.duration._

object MigrationActorSpec extends JobSpecConfig

class MigrationActorSpec extends JobSpecBase(MigrationActorSpec.getNewSystem) with BeforeAndAfter{
  private var migrationActor: ActorRef = _
  var daoActorProb: TestProbe = _
  private val testServer = new CuratorTestCluster()
  private val timeout = 60 seconds
  private val testDir = "db/jobserver-test"

  def config: Config = ConfigFactory.parseString(
    s"""
       |spark.jobserver.sqldao.jdbc.url = "jdbc:h2:mem:jobserver-migration-test;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1",
       |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}",
       |spark.jobserver.jobdao = spark.jobserver.io.CombinedDAO,
       |spark.jobserver.combineddao.metadatadao.class = spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
       |""".stripMargin
  ).withFallback(
    ConfigFactory.load("local.test.combineddao.conf")
  ).withFallback(
    ConfigFactory.load("local.test.jobsqldao.conf")
  )

  private val sqlDao = new MetaDataSqlDAO(config)
  private val zkUtils = new ZookeeperUtils(config)
  private val zkDao = new MetaDataZookeeperDAO(config)
  private val helper: SqlTestHelpers = new SqlTestHelpers(config)

  private val binary = "Some test bin".toCharArray.map(_.toByte)
  private val testInfo = BinaryInfo("test", BinaryType.Jar, DateTime.now(),
    Some(BinaryDAO.calculateBinaryHashString(binary)))
  private val testContextInfo = ContextInfo("contextId", "name", "{}", None,
    DateTime.now(), None, ContextStatus.Started, None)
  private val testJobInfo = JobInfo("jobId", "contextId", "name", testInfo,
    "", JobStatus.Running, DateTime.now(), None, None)
  private val testJobConfig = ConfigFactory.parseString("input = foo")

  before {
    migrationActor = system.actorOf(MigrationActor.props(config))
    Utils.usingResource(zkUtils.getClient) {
      client =>
        zkUtils.delete(client, "")
    }
    Await.result(helper.cleanupMetadataTables(), timeout)
  }

  describe("Should sync Zookeeper and H2") {
    it("should mirror contexts and jobs in non-final states") {
      // Prepare data: save jobs, contexts and configs in H2
      Await.result(sqlDao.saveContext(testContextInfo), timeout)
      Await.result(sqlDao.saveContext(
        testContextInfo.copy(state = ContextStatus.Finished, id = "TestId")), timeout)
      Await.result(sqlDao.saveBinary(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout)
      Await.result(sqlDao.saveJob(testJobInfo), timeout)
      Await.result(sqlDao.saveJobConfig(testJobInfo.jobId, testJobConfig), timeout)
      Await.result(sqlDao.saveJob(testJobInfo.copy(state = JobStatus.Error, jobId = "1234")), timeout)

      Await.result((migrationActor ? SyncDatabases) (timeout), timeout) should be(DatabasesSynced)

      // Check that data was written to Zookeeper
      Await.result(zkDao.getContexts(), timeout) should be(Seq(testContextInfo))
      Await.result(zkDao.getJobs(10), timeout) should be(Seq(testJobInfo))
      Await.result(zkDao.getJobConfig(testJobInfo.jobId), timeout) should be(Some(testJobConfig))
      Thread.sleep(200) // Binaries are saved in future. Prevent flaky test :)
      Await.result(zkDao.getBinary(testInfo.appName), timeout).get should be(testInfo)
    }
  }

  describe("Support request from previous migration") {
    it("should save job info in Zookeeper (which is in main DAO)") {
      Await.result(zkDao.saveBinary(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout) should be(true)
      migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(testJobInfo)
      Thread.sleep(2000)
      Await.result(zkDao.getJob(testJobInfo.jobId), timeout).get should be(testJobInfo)
    }

    it("should save job config in Zookeeper (which is in main DAO)") {
      Await.result(zkDao.saveBinary(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, testInfo.binaryStorageId.get), timeout) should be(true)
      migrationActor ! ZookeeperMigrationActor.SaveJobConfigInZK(testJobInfo.jobId, testJobConfig)
      Thread.sleep(2000)
      Await.result(zkDao.getJobConfig(testJobInfo.jobId), timeout).get should be(testJobConfig)
    }

    it("should save context info in Zookeeper (which is in main DAO)") {
      migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(testContextInfo)
      Thread.sleep(2000)
      Await.result(zkDao.getContext(testContextInfo.id), timeout).get should be(testContextInfo)
    }
  }

  describe("Test live requests") {
    it("should store binary to H2") {
      migrationActor ! MigrationActor.SaveBinaryInfoH2(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)

      Thread.sleep(2000)

      Await.result(sqlDao.getBinary("test"), 10 seconds).get should equal (testInfo)
    }

    it("should delete binary from H2") {
      Await.result(sqlDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      migrationActor ! MigrationActor.DeleteBinaryInfoH2(testInfo.appName)

      Thread.sleep(2000)

      Await.result(sqlDao.getBinary("test"), 10 seconds) should equal (None)
    }

    it("should be okay with deleting binary that doesn't exist") {
      migrationActor ! MigrationActor.DeleteBinaryInfoH2(testInfo.appName)
      Thread.sleep(2000) // wait for migration worker to save the data
      Await.result(sqlDao.getBinary("test"), 10 seconds) should equal (None)
    }

    it("should not give error if same binary is saved again") {
      migrationActor ! MigrationActor.SaveBinaryInfoH2(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)
      migrationActor ! MigrationActor.SaveBinaryInfoH2(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)

      Thread.sleep(2000)

      migrationActor ! MigrationActor.SaveBinaryInfoH2(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)
      Thread.sleep(2000)

      Await.result(sqlDao.getBinary("test"), 10 seconds).get should equal (testInfo)
    }

    it("should store context info in H2") {
      migrationActor ! MigrationActor.SaveContextInfoH2(testContextInfo)

      Thread.sleep(2000)

      Await.result(sqlDao.getContextByName(testContextInfo.name),
        10 seconds).get should equal (testContextInfo)
    }

    it("should not raise an error if same context info is saved again") {
      migrationActor ! MigrationActor.SaveContextInfoH2(testContextInfo)
      Thread.sleep(2000)

      migrationActor ! MigrationActor.SaveContextInfoH2(testContextInfo)
      Thread.sleep(2000)

      Await.result(sqlDao.getContextByName(testContextInfo.name),
        10 seconds).get should equal (testContextInfo)
    }

    it("should store job info in H2") {
      Await.result(sqlDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      migrationActor ! MigrationActor.SaveJobInfoH2(testJobInfo)

      Thread.sleep(2000)

      Await.result(sqlDao.getJob(testJobInfo.jobId), 10 seconds).get should equal (testJobInfo)
    }

    it("should not raise an error if same job info is saved again") {
      Await.result(sqlDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      migrationActor ! MigrationActor.SaveJobInfoH2(testJobInfo)
      Thread.sleep(2000)

      val testJobInfoUpdated = testJobInfo.copy(state = "ERROR")
      migrationActor ! MigrationActor.SaveJobInfoH2(testJobInfoUpdated)
      Thread.sleep(2000)

      Await.result(sqlDao.getJob(testJobInfo.jobId), 10 seconds).get should equal (testJobInfoUpdated)
    }

    it("should store job config in H2") {
      // binaryInfo and jobConfig need a job to be in DB already
      Await.result(sqlDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      Await.result(sqlDao.saveJob(testJobInfo), 10 seconds)

      migrationActor ! MigrationActor.SaveJobConfigH2(testJobInfo.jobId, testJobConfig)
      Thread.sleep(2000)

      Await.result(sqlDao.getJobConfig(testJobInfo.jobId), 10 seconds).get should equal (testJobConfig)
    }
  }
}
