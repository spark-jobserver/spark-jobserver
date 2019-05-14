package spark.jobserver.io.zookeeper

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpecLike
import org.scalatest.Matchers

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import spark.jobserver.io.BinaryDAO
import spark.jobserver.io.BinaryInfo
import spark.jobserver.io.BinaryType
import spark.jobserver.io.ContextInfo
import spark.jobserver.io.JobInfo
import spark.jobserver.util.CuratorTestCluster
import spark.jobserver.util.Utils
import spark.jobserver.util.ZKCleanup

class DAOCleanupSpec extends FunSpecLike with Matchers with BeforeAndAfter {

  /*
   * Setup
   */

  private val timeout = 60 seconds
  private val testServer = new CuratorTestCluster()

  def config: Config = ConfigFactory.parseString(
    s"""
         |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}"
    """.stripMargin
  ).withFallback(
    ConfigFactory.load("local.test.combineddao.conf")
  )

  val dao = new MetaDataZookeeperDAO(config)
  val zkUtils = new ZookeeperUtils(config)

  before {
    // Empty database
    Utils.usingResource(zkUtils.getClient) {
      client =>
        zkUtils.delete(client, "")
    }
  }

  /*
   * Test data
   */

  val date = new DateTime(1548683342369L)
  val otherDate = new DateTime(1548683342370L)
  val bin = BinaryInfo("binaryWithJar", BinaryType.Jar, date, Some(BinaryDAO.calculateBinaryHashString("1".getBytes)))

  val runningJob = JobInfo("1", "someContextId", "someContextName", bin, "someClassPath", "RUNNING", date, None, None)
  val jobWithEndtime = JobInfo("2", "someContextId", "someContextName", bin, "someClassPath", "FINISHED", date, Some(otherDate), None)
  val jobWithoutEndtime = JobInfo("3", "someContextId", "someContextName", bin, "someClassPath", "ERROR", date, None, None)

  val runningContext = ContextInfo("1", "someName1", "someConfig", Some("ActorAddress"), date, None, "RUNNING", None)
  val contextWithEndtime = ContextInfo("2", "someName2", "someConfig", Some("ActorAddress"), date, Some(otherDate), "FINISHED", None)
  val contextWithoutEndtime = ContextInfo("3", "someName3", "someConfig", Some("ActorAddress"), date, None, "ERROR", None)

  /*
   * Tests
   */

  it("Should (only) repair contexts without endtime"){
    Await.result(dao.saveBinary(bin.appName, bin.binaryType, bin.uploadTime, bin.binaryStorageId.get),
      timeout) should equal(true)
    Await.result(dao.saveContext(runningContext), timeout) should equal(true)
    Await.result(dao.saveContext(contextWithEndtime), timeout) should equal(true)
    Await.result(dao.saveContext(contextWithoutEndtime), timeout) should equal(true)
    Await.result(dao.getContexts(None, None), timeout).size should equal(3)

    val sut = new ZKCleanup(config)
    sut.cleanUpContextsNoEndTime()

    Await.result(dao.getContext("1"), timeout).get.endTime should equal(None)
    Await.result(dao.getContext("2"), timeout).get.endTime should equal(Some(otherDate))
    Await.result(dao.getContext("3"), timeout).get.endTime should equal(Some(date))
  }

  it("Should (only) repair jobs without endtime"){
    Await.result(dao.saveBinary(bin.appName, bin.binaryType, bin.uploadTime, bin.binaryStorageId.get),
      timeout) should equal(true)
    Await.result(dao.saveJob(runningJob), timeout) should equal(true)
    Await.result(dao.saveJob(jobWithEndtime), timeout) should equal(true)
    Await.result(dao.saveJob(jobWithoutEndtime), timeout) should equal(true)
    Await.result(dao.getJobs(100, None), timeout).size should equal(3)

    val sut = new ZKCleanup(config)
    sut.cleanUpJobsNoEndTime()

    Await.result(dao.getJob("1"), timeout).get.endTime should equal(None)
    Await.result(dao.getJob("2"), timeout).get.endTime should equal(Some(otherDate))
    Await.result(dao.getJob("3"), timeout).get.endTime should equal(Some(date))
  }

}