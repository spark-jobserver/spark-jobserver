package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import akka.testkit.TestProbe
import spark.jobserver.CommonMessages.{JobFinished, JobStarted}
import spark.jobserver.io.JobDAOActor.{GetJobResult, JobResult}
import spark.jobserver.io.{BinaryInfo, InMemoryBinaryDAO, InMemoryMetaDAO, JobDAOActor}

class NamedObjectsJobSpec extends JobSpecBase(JobManagerActorSpec.getNewSystem) {
  import scala.concurrent.duration._

  lazy val cfg = JobManagerActorSpec.getContextConfig(adhoc = false)

  var testBinInfo: BinaryInfo = _
  val smallTimeout = 5.seconds

  override def beforeAll() {
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryDAO
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

  private def waitAndFetchJobResult(): Array[String] = {
    expectMsgPF(smallTimeout, "Never got a JobStarted event") {
      case JobStarted(jobId, result) =>
        expectMsgClass(classOf[JobFinished])
        daoActor ! GetJobResult(jobId)
        expectMsgPF(smallTimeout, "Never got a JobResult") {
          case JobResult(result) => result match {
            case result: Array[String] => result
            case _ => throw new Exception("Unexpected result type!")
          }
        }
      case message: Any => throw new Exception(s"Got unexpected message $message")
    }
  }

  describe("NamedObjects (RDD)") {
    it("should survive from one job to another one") {
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, true), allEvents)
      val firstJobResult = waitAndFetchJobResult()
      firstJobResult should contain("rdd1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), allEvents)
      val secondJobResult = waitAndFetchJobResult()
      secondJobResult should contain("rdd1")
      secondJobResult should not contain("df1")

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1")), allEvents)
      val thirdJobResult = waitAndFetchJobResult()
      thirdJobResult should not contain("rdd1")
      thirdJobResult should not contain("df1")
    }
  }

  describe("NamedObjects (DataFrame)") {
    it("should survive from one job to another one") {

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, false), allEvents)
      val firstJobResult = waitAndFetchJobResult()
      firstJobResult should contain("df1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), allEvents)
      val secondJobResult = waitAndFetchJobResult()
      secondJobResult should equal(firstJobResult)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("df1")), allEvents)
      waitAndFetchJobResult()
    }
  }

  describe("NamedObjects (DataFrame + RDD)") {
    it("should survive from one job to another one") {
      var previousResult: Array[String] = Array.empty

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, true), allEvents)
      val firstJobResult = waitAndFetchJobResult()
      firstJobResult should contain("rdd1")
      firstJobResult should contain("df1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), allEvents)
      val secondJobResult = waitAndFetchJobResult()
      secondJobResult should equal(firstJobResult)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1", "df1")), allEvents)
      waitAndFetchJobResult()
    }
  }

  describe("NamedObjects (Broadcast)") {
    it("should survive from one job to another one") {
      var previousResult: Array[String] = Array.empty

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, true, true), allEvents)
      val firstJobResult = waitAndFetchJobResult()
      firstJobResult should contain("rdd1")
      firstJobResult should contain("df1")
      firstJobResult should contain("broadcast1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false, false), allEvents)
      val secondJobResult = waitAndFetchJobResult()
      secondJobResult should equal(firstJobResult)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1", "df1", "broadcast1")),
        allEvents)
      waitAndFetchJobResult()
    }
  }
}
