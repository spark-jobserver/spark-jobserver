package spark.jobserver

import akka.actor.ActorRef
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import spark.jobserver.io.zookeeper.{MetaDataZookeeperDAO, ZookeeperUtils}
import spark.jobserver.io._
import spark.jobserver.util.{CuratorTestCluster, Utils}

import scala.concurrent.Await
import scala.concurrent.duration._

object MigrationActorSpec extends JobSpecConfig

class ZookeeperMigrationActorSpec extends JobSpecBase(MigrationActorSpec.getNewSystem) with BeforeAndAfter{
  private var migrationActor: ActorRef = _
  var daoActorProb: TestProbe = _
  private val testServer = new CuratorTestCluster()
  private val timeout = 60 seconds

  private val testDir = "db/jobserver-test"

  def config: Config = ConfigFactory.parseString(
    s"""
       |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}",
       |spark.jobserver.zookeeperdao.dir = $testDir""".stripMargin).
    withFallback(
      ConfigFactory.load("local.test.combineddao.conf")
    )

  private val zkDao = new MetaDataZookeeperDAO(config)
  private val zkUtils = new ZookeeperUtils(config)

  private val binary = "Some test bin".toCharArray.map(_.toByte)
  private val testInfo = BinaryInfo("test", BinaryType.Jar, DateTime.now(),
    Some(BinaryDAO.calculateBinaryHashString(binary)))
  private val testContextInfo = ContextInfo("contextId", "name", "{}", None,
    DateTime.now(), None, ContextStatus.Started, None)
  private val testJobInfo = JobInfo("jobId", "contextId", "name", testInfo,
    "", JobStatus.Running, DateTime.now(), None, None)
  private val testJobConfig = ConfigFactory.parseString("input = foo")

  before {
    daoActorProb = TestProbe()
    migrationActor = system.actorOf(ZookeeperMigrationActor.props(config))
  }

  after {
    Utils.usingResource(zkUtils.getClient) {
      client => zkUtils.delete(client, "")
    }
  }

  describe("Test live requests") {
    it("should store binary to Zookeeper") {
      migrationActor ! ZookeeperMigrationActor.SaveBinaryInfoInZK(testInfo.appName, BinaryType.Jar,
        testInfo.uploadTime, binary)

      Thread.sleep(2000)

      Await.result(zkDao.getBinary("test"), 10 seconds).get should equal (testInfo)
    }

    it("should delete binary from Zookeeper") {
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      migrationActor ! ZookeeperMigrationActor.DeleteBinaryInfoFromZK(testInfo.appName)

      Thread.sleep(2000)

      Await.result(zkDao.getBinary("test"), 10 seconds) should equal (None)
    }

    it("should be okay with deleting binary that doesn't exist") {
      migrationActor ! ZookeeperMigrationActor.DeleteBinaryInfoFromZK(testInfo.appName)
      Thread.sleep(2000) // wait for migration worker to save the data
      Await.result(zkDao.getBinary("test"), 10 seconds) should equal (None)
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

      Await.result(zkDao.getBinary("test"), 10 seconds).get should equal (testInfo)
    }

    it("should store context info in Zookeeper") {
      migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(testContextInfo)

      Thread.sleep(2000)

      Await.result(zkDao.getContextByName(testContextInfo.name),
        10 seconds).get should equal (testContextInfo)
    }

    it("should not raise an error if same context info is saved again") {
      migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(testContextInfo)
      Thread.sleep(2000)

      migrationActor ! ZookeeperMigrationActor.SaveContextInfoInZK(testContextInfo)
      Thread.sleep(2000)

      Await.result(zkDao.getContextByName(testContextInfo.name),
        10 seconds).get should equal (testContextInfo)
    }

    it("should store job info in Zookeeper") {
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(testJobInfo)

      Thread.sleep(2000)

      Await.result(zkDao.getJob(testJobInfo.jobId), 10 seconds).get should equal (testJobInfo)
    }

    it("should not raise an error if same job info is saved again") {
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(testJobInfo)
      Thread.sleep(2000)

      val testJobInfoUpdated = testJobInfo.copy(state = "ERROR")
      migrationActor ! ZookeeperMigrationActor.SaveJobInfoInZK(testJobInfoUpdated)
      Thread.sleep(2000)

      Await.result(zkDao.getJob(testJobInfo.jobId), 10 seconds).get should equal (testJobInfoUpdated)
    }

    it("should store job config in Zookeeper") {
      // binaryInfo and jobConfig need a job to be in DB already
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      Await.result(zkDao.saveJob(testJobInfo), 10 seconds)

      migrationActor ! ZookeeperMigrationActor.SaveJobConfigInZK(testJobInfo.jobId, testJobConfig)
      Thread.sleep(2000)

      Await.result(zkDao.getJobConfig(testJobInfo.jobId), 10 seconds).get should equal (testJobConfig)
    }

    it("should not raise an error if same job config is saved again") {
      // binaryInfo and jobConfig need a job to be in DB already
      Await.result(zkDao.saveBinary(testInfo.appName, testInfo.binaryType,
        testInfo.uploadTime, testInfo.binaryStorageId.get), 10 seconds)
      Await.result(zkDao.saveJob(testJobInfo), 10 seconds)

      migrationActor ! ZookeeperMigrationActor.SaveJobConfigInZK(testJobInfo.jobId, testJobConfig)
      Thread.sleep(2000)

      val anotherTestJobConfig = ConfigFactory.parseString("foo = bar")
      migrationActor ! ZookeeperMigrationActor.SaveJobConfigInZK(testJobInfo.jobId, anotherTestJobConfig)
      Thread.sleep(2000)

      Await.result(zkDao.getJobConfig(testJobInfo.jobId), 10 seconds).get should equal (anotherTestJobConfig)
    }
  }
}
