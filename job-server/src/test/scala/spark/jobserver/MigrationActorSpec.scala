package spark.jobserver

import akka.actor.ActorRef
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import spark.jobserver.io._
import spark.jobserver.util.CuratorTestCluster

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
       |""".stripMargin
  ).withFallback(
    ConfigFactory.load("local.test.jobsqldao.conf")
  )

  private val sqlDao = new MetaDataSqlDAO(config)

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
