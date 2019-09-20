package spark.jobserver.io.zookeeper

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpecLike
import org.scalatest.Matchers
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import spark.jobserver.io._
import spark.jobserver.util.{CuratorTestCluster, NoCorrespondingContextAliveException, Utils, ZKCleanup}

class DAOCleanupSpec extends FunSpecLike with Matchers with BeforeAndAfter {

  /*
   * Setup
   */

  private val timeout = 60 seconds
  private val testServer = new CuratorTestCluster()

  def config: Config = ConfigFactory.parseString(
    s"""
         |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}"
         |spark.jobserver.dao-timeout = 3s
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

  val runningJob = JobInfo("1", "someContextId", "someContextName",
    "someClassPath", "RUNNING", date, None, None, Seq(bin))
  val jobWithEndtime = JobInfo("2", "someContextId", "someContextName",
    "someClassPath", "FINISHED", date, Some(otherDate), None, Seq(bin))
  val jobWithoutEndtime = JobInfo("3", "someContextId",
    "someContextName", "someClassPath", "ERROR", date, None, None, Seq(bin))

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

  describe("Non final jobs with final context tests") {
    it("should not fail if nothing to repair") {
      (new ZKCleanup(config)).cleanupNonFinalJobsWithFinalContext() should be(true)
    }

    it("should not repair jobs whose context are in non-final state") {
      val runningContextInfo = runningContext
      val runningJobInfo = runningJob.copy(
        contextId = runningContext.id, contextName = runningContext.name)
      val errorJobInfo = runningJobInfo.copy(jobId = "eId", state = JobStatus.Error)
      Await.result(dao.saveContext(runningContextInfo), timeout) should equal(true)
      Await.result(dao.saveJob(runningJobInfo), timeout) should equal(true)
      Await.result(dao.saveJob(errorJobInfo), timeout) should equal(true)

      (new ZKCleanup(config)).cleanupNonFinalJobsWithFinalContext() should be(true)

      Await.result(dao.getJob(runningJobInfo.jobId), timeout).get should be(runningJobInfo)
      Await.result(dao.getJob(errorJobInfo.jobId), timeout).get should be(errorJobInfo)
    }

    it("should repair job whose context is not running") {
      val finishedContextInfo = contextWithEndtime
      val runningJobInfo = runningJob.copy(
        contextId = finishedContextInfo.id, contextName = finishedContextInfo.name)
      Await.result(dao.saveContext(finishedContextInfo), timeout) should equal(true)
      Await.result(dao.saveJob(runningJobInfo), timeout) should equal(true)

      (new ZKCleanup(config)).cleanupNonFinalJobsWithFinalContext() should be(true)

      checkIfJobIsCleaned(runningJobInfo.jobId)
    }

    it("should repair all jobs whose corresponding context is not running") {
      val finishedContextInfo = contextWithEndtime
      val runningJobInfo = runningJob.copy(
        contextId = finishedContextInfo.id, contextName = finishedContextInfo.name)
      val finishedJobInfo = runningJobInfo.copy(jobId = "fId", state = JobStatus.Finished)
      val errorJobInfo = runningJobInfo.copy(jobId = "eId", state = JobStatus.Error)
      val restartingJobInfo = runningJobInfo.copy(jobId = "rId", state = JobStatus.Restarting)

      Await.result(dao.saveContext(finishedContextInfo), timeout) should equal(true)
      Await.result(dao.saveJob(runningJobInfo), timeout) should equal(true)
      Await.result(dao.saveJob(finishedJobInfo), timeout) should equal(true)
      Await.result(dao.saveJob(errorJobInfo), timeout) should equal(true)
      Await.result(dao.saveJob(restartingJobInfo), timeout) should equal(true)

      (new ZKCleanup(config)).cleanupNonFinalJobsWithFinalContext() should be(true)

      checkIfJobIsCleaned(runningJobInfo.jobId)
      checkIfJobIsCleaned(restartingJobInfo.jobId)
      Await.result(dao.getJob(finishedJobInfo.jobId), timeout).get should be(finishedJobInfo)
      Await.result(dao.getJob(errorJobInfo.jobId), timeout).get should be(errorJobInfo)
    }

    it("should repair all jobs even if they belong to different contexts") {
      val finishedContextInfo = contextWithEndtime
      val erroredContextInfo = contextWithEndtime.copy(id = "eCtx", state = ContextStatus.Error)
      val runningJobInfo = runningJob.copy(
        contextId = finishedContextInfo.id, contextName = finishedContextInfo.name)
      val runningJobInfo2 = runningJobInfo.copy(jobId = "jId2")
      Await.result(dao.saveContext(finishedContextInfo), timeout) should equal(true)
      Await.result(dao.saveContext(erroredContextInfo), timeout) should equal(true)
      Await.result(dao.saveJob(runningJobInfo), timeout) should equal(true)
      Await.result(dao.saveJob(runningJobInfo2), timeout) should equal(true)

      (new ZKCleanup(config)).cleanupNonFinalJobsWithFinalContext() should be(true)

      checkIfJobIsCleaned(runningJobInfo.jobId)
      checkIfJobIsCleaned(runningJobInfo2.jobId)
    }

    it("should not throw exception if dao operations are failing") {
      val wrongConfig = ConfigFactory.parseString(
        s"""
           |spark.jobserver.zookeeperdao.connection-string = "abc"
           |spark.jobserver.dao-timeout = 3s
    """.stripMargin).withFallback(
        ConfigFactory.load("local.test.combineddao.conf")
      )
      new ZKCleanup(wrongConfig).cleanupNonFinalJobsWithFinalContext() should be(false)
    }

    it("should return true if context is not found") {
      new ZKCleanup(config).isContextStateFinal("doesnt-exist") should be(true)
    }

    it("should return true if failed to fetch context") {
      val wrongConfig = ConfigFactory.parseString(
        s"""
           |spark.jobserver.zookeeperdao.connection-string = "abc"
           |spark.jobserver.dao-timeout = 3s
    """.stripMargin).withFallback(
        ConfigFactory.load("local.test.combineddao.conf")
      )

      Await.result(dao.saveContext(contextWithEndtime), timeout) should equal(true)

      new ZKCleanup(wrongConfig).isContextStateFinal(contextWithEndtime.id) should be(true)
    }

    it("should return true if context is in final state") {
      Await.result(dao.saveContext(contextWithEndtime), timeout) should equal(true)

      new ZKCleanup(config).isContextStateFinal(contextWithEndtime.id) should be(true)
    }

    it("should return false if context is in final state") {
      Await.result(dao.saveContext(runningContext), timeout) should equal(true)

      new ZKCleanup(config).isContextStateFinal(runningContext.id) should be(false)
    }
  }

  private def checkIfJobIsCleaned(jobId: String) = {
    val updatedJob = Await.result(dao.getJob(jobId), timeout).get
    updatedJob.state should equal(JobStatus.Error)
    updatedJob.endTime should not be (None)
    updatedJob.error.get.message should be(
      NoCorrespondingContextAliveException(updatedJob.jobId).getMessage)
  }
}