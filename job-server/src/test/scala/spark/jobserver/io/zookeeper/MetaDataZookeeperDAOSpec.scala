package spark.jobserver.io.zookeeper

import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpec, FunSpecLike, Matchers}
import spark.jobserver.io.{BinaryDAO, BinaryInfo, BinaryType, ContextInfo, ErrorData, JobInfo}
import spark.jobserver.util.CuratorTestCluster
import spark.jobserver.TestJarFinder

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import spark.jobserver.util.Utils

class MetaDataZookeeperDAOSpec extends FunSpec with TestJarFinder with FunSpecLike
      with Matchers with BeforeAndAfter {

  /*
   * Setup
   */

  private val timeout = 60 seconds

  private val testServer = new CuratorTestCluster()
  private val testDir = "jobserver-test"
  def config: Config = ConfigFactory.parseString(
    s"""
         |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}",
         |spark.jobserver.zookeeperdao.dir = $testDir""".stripMargin)

  private var dao = new MetaDataZookeeperDAO(config)
  private val zkUtils = new ZookeeperUtils(testServer.getConnectString, testDir, 1)

  before {
    testServer.createBaseDir(testDir)
    Utils.usingResource(zkUtils.getClient) {
      client =>
        zkUtils.delete(client, "/")
    }
  }

  /*
   * Test data
   */

  // Binaries
  val binJar = BinaryInfo("binaryWithJar", BinaryType.Jar, new DateTime(),
      Some(BinaryDAO.calculateBinaryHashString("1".getBytes)))
  val binEgg = BinaryInfo("binaryWithEgg", BinaryType.Egg, new DateTime(),
      Some(BinaryDAO.calculateBinaryHashString("2".getBytes)))
  val binJarV2 = BinaryInfo("binaryWithJar", BinaryType.Jar, new DateTime().plusHours(1),
      Some(BinaryDAO.calculateBinaryHashString("3".getBytes)))
  val binElse = BinaryInfo("anotherBinaryWithJar", BinaryType.Jar, new DateTime(),
      Some(BinaryDAO.calculateBinaryHashString("3".getBytes)))

  // Contexts
  val normalContext = ContextInfo("someId", "someName", "someConfig", Some("ActorAddress"),
      new DateTime(1548683342369L), Some(new DateTime(1548683342370L)), "someState",
      Some(new Throwable("message")))
  val sameIdContext = ContextInfo("someId", "someOtherName", "someOtherConfig",
      Some("OtherActorAddress"), new DateTime(1548683342369L),
      Some(new DateTime(1548683342370L)), "someOtherState", Some(new Throwable("otherMessage")))
  val minimalContext = new ContextInfo("someOtherId", "someName", "someOtherconfig", None,
      new DateTime(1548683342368L), None, "someState", None)
  val anotherStateContext = new ContextInfo("anotherId", "anotherName", "someOtherconfig", None,
      new DateTime(1548683342368L).plusHours(1), None, "someState", None)

  // Jobs
  val normalJob = JobInfo("someJobId", "someContextId", "someContextName", binJar,
      "someClassPath", "someState", new DateTime(), Some(new DateTime()),
      Some(ErrorData("someMessage", "someError", "someTrace")))
  val minimalJob = JobInfo("someOtherJobId", "someContextId", "someOtherContextName", binJar,
      "someClassPath", "someState", new DateTime().plusHours(1), None, None)
  val sameIdJob = JobInfo("someJobId", "someOtherContextId", "thirdContextName", binJar,
      "someClassPath", "someState", new DateTime().minusHours(1), None, None)
  val anotherJob = JobInfo("thirdJobId", "someOtherContextId", "thirdContextName", binJar,
      "someClassPath", "anotherState", new DateTime().minusHours(1), None, None)

  // JobConfigs
  val config1 = ConfigFactory.parseString("{key : value}")
  val config2 = ConfigFactory.parseString("{key : value2}")

  /*
   * Tests: Binaries
   */

  describe("Binary tests") {

    it("should save and retrieve binaries") {
      // JAR
      var success = Await.result(dao.saveBinary(binJar.appName, binJar.binaryType,
          binJar.uploadTime, binJar.binaryStorageId.get), timeout)
      success should equal(true)
      val binJarStored = Await.result(dao.getBinary(binJar.appName), timeout)
      binJarStored should equal(Some(binJar))
      // EGG
      success = Await.result(dao.saveBinary(binEgg.appName, binEgg.binaryType,
          binEgg.uploadTime, binEgg.binaryStorageId.get), timeout)
      success should equal(true)
      val binEggStored = Await.result(dao.getBinary(binEgg.appName), timeout)
      binEggStored should equal(Some(binEgg))
    }

    it("should retrieve the last uploaded binary") {
      // Save two binaries with same name (one older)
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType, binJar.uploadTime,
          binJar.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binJarV2.appName, binJarV2.binaryType, binJarV2.uploadTime,
          binJarV2.binaryStorageId.get), timeout) should equal(true)
      // Get the older one
      val binStored = Await.result(dao.getBinary(binJarV2.appName), timeout)
      binStored should equal(Some(binJarV2))
    }

    it("should return none if there is no binary with that name") {
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType, binJar.uploadTime,
          binJar.binaryStorageId.get), timeout) should equal(true)
      val binStored = Await.result(dao.getBinary("nonexisting name"), timeout)
      binStored should equal(None)
    }

    it("should list all (oldest) binaries") {
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType, binJar.uploadTime,
          binJar.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binJarV2.appName, binJarV2.binaryType, binJarV2.uploadTime,
          binJarV2.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binEgg.appName, binEgg.binaryType, binEgg.uploadTime,
          binEgg.binaryStorageId.get), timeout) should equal(true)
      val allBins = Await.result(dao.getBinaries, timeout)
      allBins.toSet should equal(Set(binJarV2, binEgg))
    }

    it("should return empty list if there are no binaries") {
      val allBins = Await.result(dao.getBinaries, timeout)
      allBins should equal(Seq.empty[BinaryInfo])
    }

    it("should delete binaries") {
      // Insert three binaries
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType, binJar.uploadTime,
          binJar.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binJarV2.appName, binJarV2.binaryType, binJarV2.uploadTime,
          binJarV2.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binEgg.appName, binEgg.binaryType, binEgg.uploadTime,
          binEgg.binaryStorageId.get), timeout) should equal(true)
      // Delete two with same name
      Await.result(dao.deleteBinary(binJar.appName), timeout) should equal(true)
      // Assertions
      Await.result(dao.getBinaries, timeout) should equal(Seq(binEgg))
      Await.result(dao.getBinary(binJar.appName), timeout) should equal(None)
      Await.result(dao.getBinary(binEgg.appName), timeout) should equal(Some(binEgg))
    }

    it("should delete nothing if deleting a non-existing object") {
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType, binJar.uploadTime,
          binJar.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.deleteBinary("nonexisting name"), timeout) should equal(false)
      Await.result(dao.getBinaries, timeout) should equal(Seq(binJar))
      Await.result(dao.getBinary(binJar.appName), timeout) should equal(Some(binJar))
    }

    it("should return all binaries given a storage id") {
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType, binJar.uploadTime,
          binJar.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binJarV2.appName, binJarV2.binaryType, binJarV2.uploadTime,
          binJarV2.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binEgg.appName, binEgg.binaryType, binEgg.uploadTime,
          binEgg.binaryStorageId.get), timeout) should equal(true)
      Await.result(dao.saveBinary(binElse.appName, binElse.binaryType, binElse.uploadTime,
          binElse.binaryStorageId.get), timeout) should equal(true)
      val binsOnStor3 = Await.result(dao.getBinariesByStorageId(
          BinaryDAO.calculateBinaryHashString("3".getBytes)), timeout)
      binsOnStor3.toSet should equal(Set(binJarV2, binElse))
    }

    it("should list all binaries with given storage id with unique (name, binaryStorageId)") {
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType,
        DateTime.now(), binJar.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType,
        binJar.uploadTime, binJar.binaryStorageId.get), timeout)
      Await.result(dao.saveBinary(binJar.appName + "2", binJar.binaryType,
        binJar.uploadTime, "anotherStorageId"), timeout)
      val resultList = Await.result(dao.getBinariesByStorageId(binJar.binaryStorageId.get), timeout)
      resultList.length should equal(1)
      resultList.head.appName should equal(binJar.appName)
    }

  }

  /*
   * Tests: Contexts
   */

  describe("Context tests") {

    it("should save and retrieve contexts") {
      Await.result(dao.saveContext(normalContext), timeout) should equal(true)
      Await.result(dao.getContext(normalContext.id), timeout) should equal(Some(normalContext))
    }

    it("should return None if there is no context with a given id") {
      Await.result(dao.saveContext(normalContext), timeout)
      Await.result(dao.getContext("someOtherContextId"), timeout) should equal(None)
    }

    it("should update a context if saved with the same id") {
      Await.result(dao.saveContext(sameIdContext), timeout) should equal(true)
      Await.result(dao.saveContext(normalContext), timeout) should equal(true)
      Await.result(dao.getContext(normalContext.id), timeout) should equal(Some(normalContext))

    }
    it("should retrieve contexts by their name") {
      Await.result(dao.saveContext(normalContext), timeout)
      Await.result(dao.saveContext(minimalContext), timeout)
      Await.result(dao.getContextByName("someName"), timeout) should equal(Some(normalContext))
      Await.result(dao.getContextByName("someNonExistingName"), timeout) should equal(None)
    }

    it("should retrieve all contexts") {
      Await.result(dao.saveContext(normalContext), timeout)
      Await.result(dao.saveContext(sameIdContext), timeout)
      Await.result(dao.saveContext(minimalContext), timeout)
      val jobs = Await.result(dao.getContexts(None, None), timeout)
      jobs.size should equal(2)
      jobs should equal(Seq(sameIdContext, minimalContext))
      Await.result(dao.getContexts(Some(100), None), timeout) should equal(jobs)
    }

    it("should retrieve all contexts with a limit") {
      Await.result(dao.saveContext(normalContext), timeout)
      Await.result(dao.saveContext(sameIdContext), timeout)
      Await.result(dao.saveContext(minimalContext), timeout)
      Await.result(dao.getContexts(Some(1), None), timeout) should equal(Seq(sameIdContext))
    }

    it("should retrieve all contexts filtered by a state") {
      Await.result(dao.saveContext(normalContext), timeout)
      Await.result(dao.saveContext(sameIdContext), timeout)
      Await.result(dao.saveContext(minimalContext), timeout)
      Await.result(dao.saveContext(anotherStateContext), timeout)
      val jobs = Await.result(dao.getContexts(None, Some(Seq("someState", "nonexistingState"))),
          timeout)
      jobs should equal(Seq(anotherStateContext, minimalContext))
    }

    it("should retrieve all contexts limited and filtered by a state") {
      Await.result(dao.saveContext(normalContext), timeout)
      Await.result(dao.saveContext(sameIdContext), timeout)
      Await.result(dao.saveContext(minimalContext), timeout)
      Await.result(dao.saveContext(anotherStateContext), timeout)
      val jobs = Await.result(dao.getContexts(Some(1), Some(Seq("someState", "nonexistingState"))),
          timeout)
      jobs should equal(Seq(anotherStateContext))
    }

  }

  /*
   * Tests: Jobs
   */

  describe("Job tests") {

    def insertInitialBinary() : Unit = { Await.result(dao.saveBinary(binJar.appName, binJar.binaryType,
        binJar.uploadTime, binJar.binaryStorageId.get), timeout) }

    it("should save and retrieve job info properly") {
      insertInitialBinary()
      val success = Await.result(dao.saveJob(normalJob), timeout)
      success should equal(true)
      Await.result(dao.getJob("someJobId"), timeout) should equal(Some(normalJob))
    }

    it("should return None if there is no job with a given id") {
      insertInitialBinary()
      Await.result(dao.saveJob(normalJob), timeout)
      Await.result(dao.getJob("someOtherJobId"), timeout) should equal(None)
    }

    it("should update a job if saved with the same id") {
      insertInitialBinary()
      var success = Await.result(dao.saveJob(sameIdJob), timeout)
      success should equal(true)
      success = Await.result(dao.saveJob(normalJob), timeout)
      success should equal(true)
      Await.result(dao.getJob("someJobId"), timeout) should equal(Some(normalJob))
    }

    it("should retrieve jobs by their context id") {
      insertInitialBinary()
      Await.result(dao.saveJob(normalJob), timeout)
      Await.result(dao.saveJob(minimalJob), timeout)
      Await.result(dao.saveJob(anotherJob), timeout)
      val jobs = Await.result(dao.getJobsByContextId("someContextId", None), timeout)
      jobs.size should equal(2)
      jobs should equal(Seq(minimalJob, normalJob)) //right order?
    }

    it("should retrieve jobs by their context id filtered by states") {
      insertInitialBinary()
      Await.result(dao.saveJob(normalJob), timeout)
      Await.result(dao.saveJob(sameIdJob), timeout)
      Await.result(dao.saveJob(minimalJob), timeout)
      Await.result(dao.saveJob(anotherJob), timeout)
      val jobs = Await.result(dao.getJobsByContextId("someOtherContextId", Some(Seq("someState"))),
          timeout)
      jobs should equal(Seq(sameIdJob))
    }

    it("should retrieve all jobs") {
      insertInitialBinary()
      Await.result(dao.saveJob(normalJob), timeout)
      Await.result(dao.saveJob(minimalJob), timeout)
      Await.result(dao.saveJob(anotherJob), timeout)
      val jobs = Await.result(dao.getJobs(100, None), timeout)
      jobs.size should equal(3)
    }

    it("should retrieve all jobs with a limit") {
      insertInitialBinary()
      Await.result(dao.saveJob(normalJob), timeout)
      Await.result(dao.saveJob(minimalJob), timeout)
      Await.result(dao.saveJob(anotherJob), timeout)
      val jobs = Await.result(dao.getJobs(1, None), timeout)
      jobs should equal(Seq(minimalJob))
    }

    it("should retrieve all jobs with a limit filtered by a state") {
      insertInitialBinary()
      Await.result(dao.saveJob(normalJob), timeout)
      Await.result(dao.saveJob(minimalJob), timeout)
      Await.result(dao.saveJob(anotherJob), timeout)
      val jobs = Await.result(dao.getJobs(1, Some("someState")), timeout)
      jobs should equal(Seq(minimalJob))
    }

    it("should not retrieve jobs with missing binaries") {
      val success = Await.result(dao.saveJob(normalJob), timeout)
      success should equal(true)
      Await.result(dao.getJob("someJobId"), timeout) should equal(None)
      Await.result(dao.getJobs(100, None), timeout).size should equal(0)
    }

    it("should be able to access data after DAO recreation") {
      insertInitialBinary()
      // Save a job
      val success = Await.result(dao.saveJob(normalJob), timeout)
      success should equal(true)
      Await.result(dao.getJob("someJobId"), timeout) should equal(Some(normalJob))
      // Restart
      dao = null
      dao = new MetaDataZookeeperDAO(config)
      // Still available?
      Await.result(dao.getJob("someJobId"), timeout) should equal(Some(normalJob))
    }

  }

  /*
   * Tests: JobConfigs
   */

  describe("JobConfig tests") {

    def insertInitialBinaryAndJob() : Unit = {
      Await.result(dao.saveBinary(binJar.appName, binJar.binaryType, binJar.uploadTime,
          binJar.binaryStorageId.get), timeout)
      Await.result(dao.saveJob(normalJob), timeout)
    }

    it("should save and read job config") {
      insertInitialBinaryAndJob()
      Await.result(dao.saveJobConfig("someJobId", config1), timeout) should equal(true)
      Await.result(dao.getJobConfig("someJobId"), timeout) should equal(Some(config1))
    }

    it("should return None for a non-existig config") {
      insertInitialBinaryAndJob()
      Await.result(dao.getJobConfig("someJobId"), timeout) should equal(None)
    }

    it("should write a config for non-existing job") {
      Await.result(dao.saveJobConfig("abcde", config1), timeout) should equal(true)
      Await.result(dao.getJobConfig("abcde"), timeout) should equal(Some(config1))
    }

    it("should update configs on a second write call") {
      insertInitialBinaryAndJob()
      Await.result(dao.saveJobConfig("someJobId", config1), timeout) should equal(true)
      Await.result(dao.getJobConfig("someJobId"), timeout) should equal(Some(config1))
      Await.result(dao.saveJobConfig("someJobId", config2), timeout) should equal(true)
      Await.result(dao.getJobConfig("someJobId"), timeout) should equal(Some(config2))
    }

  }

}
