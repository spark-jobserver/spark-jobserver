package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import akka.testkit.TestProbe
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.io.{BinaryInfo, InMemoryBinaryDAO, InMemoryMetaDAO, JobDAOActor}

class NamedObjectsJobSpec extends JobSpecBase(JobManagerActorSpec.getNewSystem) {
  import scala.concurrent.duration._

  lazy val cfg = JobManagerActorSpec.getContextConfig(adhoc = false)

  var testBinInfo: BinaryInfo = _

  override def beforeAll() {
    inMemoryMetaDAO = new InMemoryMetaDAO
    inMemoryBinDAO = new InMemoryBinaryDAO
    daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))
    manager = system.actorOf(JobManagerActor.props(daoActor))
    supervisor = TestProbe().ref

    manager ! JobManagerActor.Initialize(cfg, None, emptyActor)

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

  describe("NamedObjects (RDD)") {
    it("should survive from one job to another one") {
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, true), errorEvents ++ syncEvents)
      val JobResult(_, names: Array[String]) = expectMsgClass(classOf[JobResult])
      names should contain("rdd1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), errorEvents ++ syncEvents)
      val JobResult(_, names2: Array[String]) = expectMsgClass(classOf[JobResult])

      names2 should contain("rdd1")
      names2 should not contain("df1")

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1")), errorEvents ++ syncEvents)
      val JobResult(_, names3: Array[String]) = expectMsgClass(classOf[JobResult])

      names3 should not contain("rdd1")
      names3 should not contain("df1")
    }
  }

  describe("NamedObjects (DataFrame)") {
    it("should survive from one job to another one") {
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, false), errorEvents ++ syncEvents)
      // for some reason, this just needs some more time to finish occasinally
      val JobResult(_, names: Array[String]) = expectMsgClass(10.seconds, classOf[JobResult])

      names should contain("df1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), errorEvents ++ syncEvents)
      val JobResult(_, names2: Array[String]) = expectMsgClass(classOf[JobResult])

      names2 should equal(names)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("df1")), errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobResult])
    }
  }

  describe("NamedObjects (DataFrame + RDD)") {
    it("should survive from one job to another one") {
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, true), errorEvents ++ syncEvents)
      val JobResult(_, names: Array[String]) = expectMsgClass(classOf[JobResult])

      names should contain("rdd1")
      names should contain("df1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false), errorEvents ++ syncEvents)
      val JobResult(_, names2: Array[String]) = expectMsgClass(classOf[JobResult])

      names2 should equal(names)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1", "df1")), errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobResult])
    }
  }

  describe("NamedObjects (Broadcast)") {
    it("should survive from one job to another one") {

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(true, true, true), errorEvents ++ syncEvents)
      val JobResult(_, names: Array[String]) = expectMsgClass(classOf[JobResult])

      names should contain("rdd1")
      names should contain("df1")
      names should contain("broadcast1")

      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getCreateConfig(false, false, false), errorEvents ++ syncEvents)
      val JobResult(_, names2: Array[String]) = expectMsgClass(classOf[JobResult])

      names2 should equal(names)

      //clean-up
      manager ! JobManagerActor.StartJob(
        jobName, Seq(testBinInfo), getDeleteConfig(List("rdd1", "df1", "broadcast1")),
        errorEvents ++ syncEvents)
      expectMsgClass(classOf[JobResult])
    }
  }
}
