package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import akka.testkit.TestProbe
import spark.jobserver.io.{BinaryInfo, InMemoryBinaryObjectsDAO, InMemoryMetaDAO, JobDAOActor}

class NamedObjectsJobSpec extends JobSpecBase(JobManagerActorSpec.getNewSystem) {
  import scala.concurrent.duration._

  lazy val cfg = JobManagerActorSpec.getContextConfig(adhoc = false)

  var testBinInfo: BinaryInfo = _

  override def beforeAll() {
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryObjectsDAO
    daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
    manager = system.actorOf(JobManagerActor.props(daoActor))
    supervisor = TestProbe().ref

    manager ! JobManagerActor.Initialize(cfg, emptyActor)

    expectMsgClass(10.seconds, classOf[JobManagerActor.Initialized])

    testBinInfo = uploadTestJar()
  }

  val jobName = "spark.jobserver.NamedObjectsTestJob"

  private def getCreateConfig(createDF: Boolean, createRDD: Boolean, createBroadcast: Boolean = false) : Config = {
    ConfigFactory.parseString("spark.jobserver.named-object-creation-timeout = 60 s, " +
        NamedObjectsTestJobConfig.CREATE_DF + " = " + createDF + ", " +
        NamedObjectsTestJobConfig.CREATE_RDD + " = " + createRDD + ", " +
        NamedObjectsTestJobConfig.CREATE_BROADCAST + " = " + createBroadcast)
  }

  private def getDeleteConfig(names: List[String]) : Config = {
    ConfigFactory.parseString("spark.jobserver.named-object-creation-timeout = 60 s, " +
        NamedObjectsTestJobConfig.DELETE+" = [" + names.mkString(", ") + "]")
  }

  private def waitAndFetchJobResultArray(jobFinishTimeout: FiniteDuration = 10 seconds): Array[String] = {
    super.waitAndFetchJobResult(jobFinishTimeout).asInstanceOf[Array[String]]
  }

  describe("NamedObjects (RDD)") {
    it("should survive from one job to another one") {
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, true), allEvents)
      val firstJobResult = waitAndFetchJobResultArray()
      firstJobResult should contain("rdd1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), allEvents)
      val secondJobResult = waitAndFetchJobResultArray()
      secondJobResult should contain("rdd1")
      secondJobResult should not contain("df1")

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1")), allEvents)
      val thirdJobResult = waitAndFetchJobResultArray()
      thirdJobResult should not contain("rdd1")
      thirdJobResult should not contain("df1")
    }
  }

  describe("NamedObjects (DataFrame)") {
    it("should survive from one job to another one") {

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, false), allEvents)
      val firstJobResult = waitAndFetchJobResultArray()
      firstJobResult should contain("df1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), allEvents)
      val secondJobResult = waitAndFetchJobResultArray()
      secondJobResult should equal(firstJobResult)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("df1")), allEvents)
      waitAndFetchJobResultArray()
    }
  }

  describe("NamedObjects (DataFrame + RDD)") {
    it("should survive from one job to another one") {
      var previousResult: Array[String] = Array.empty

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, true), allEvents)
      val firstJobResult = waitAndFetchJobResultArray()
      firstJobResult should contain("rdd1")
      firstJobResult should contain("df1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), allEvents)
      val secondJobResult = waitAndFetchJobResultArray()
      secondJobResult should equal(firstJobResult)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1", "df1")), allEvents)
      waitAndFetchJobResultArray()
    }
  }

  describe("NamedObjects (Broadcast)") {
    it("should survive from one job to another one") {
      var previousResult: Array[String] = Array.empty

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, true, true), allEvents)
      val firstJobResult = waitAndFetchJobResultArray()
      firstJobResult should contain("rdd1")
      firstJobResult should contain("df1")
      firstJobResult should contain("broadcast1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false, false), allEvents)
      val secondJobResult = waitAndFetchJobResultArray()
      secondJobResult should equal(firstJobResult)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1", "df1", "broadcast1")),
        allEvents)
      waitAndFetchJobResultArray()
    }
  }
}
