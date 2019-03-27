package spark.jobserver.io

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.Config
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.io.JobDAOActor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.util.NoMatchingDAOObjectException

object JobDAOActorSpec {
  val system = ActorSystem("dao-test")
  val dt = DateTime.now()
  val dtplus1 = dt.plusHours(1)

  val cleanupProbe = TestProbe()(system)
  val unblockingProbe = TestProbe()(system)
  val spyProbe = TestProbe()(system)

  object DummyDao extends JobDAO{
    override def saveBinary(appName: String, binaryType: BinaryType,
                            uploadTime: DateTime, binaryBytes: Array[Byte]): Unit = {
      appName match {
        case "failOnThis" => throw new Exception("deliberate failure")
        case "blockDAO" => unblockingProbe.expectMsg(5.seconds, "unblock")
        case _ => //Do nothing
      }
    }

    override def getApps: Future[Map[String, (BinaryType, DateTime)]] =
      Future.successful(Map(
        "app1" -> (BinaryType.Jar, dt),
        "app2" -> (BinaryType.Egg, dtplus1)
      ))

    override def getBinaryFilePath(appName: String,
                                   binaryType: BinaryType, uploadTime: DateTime): String = ???

    override def saveContextInfo(contextInfo: ContextInfo): Unit = {
      contextInfo.id match {
        case "success" =>
        case "update-running" | "update-with-address" | "update-non-final" =>
          spyProbe.ref ! contextInfo
          unblockingProbe.expectMsg("unblock")
        case "failure" | "update-fail" => throw new Exception("deliberate failure")
      }
    }

    override def getContextInfo(id: String): Future[Option[ContextInfo]] = {
      id match {
        case "update-running" => Future.successful(
          Option(ContextInfo("update-running", "name", "config", None,
            DateTime.now(), None, ContextStatus.Running, None)))
        case "update-with-address" => Future.successful(
          Option(ContextInfo("update-with-address", "name", "config", Some("address"),
            DateTime.now(), None, ContextStatus.Running, None)))
        case "update-non-final" => Future.successful(
          Option(ContextInfo("update-non-final", "name", "config", None,
            DateTime.now(), None, ContextStatus.Stopping, None)))
        case "update-dao-fail" => Future.failed(new Exception("deliberate failure"))
        case "update-not-found" => Future.successful(None)
        case "update-fail" => Future.successful(
          Option(ContextInfo("update-fail", "name", "config", None,
            DateTime.now(), None, ContextStatus.Running, None)))
        case _ => Future.successful(None)
      }
    }

    override def getContextInfos(limit: Option[Int] = None, statuses: Option[Seq[String]] = None):
      Future[Seq[ContextInfo]] = ???

    override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = ???

    override def saveJobConfig(jobId: String, jobConfig: Config): Unit = ???

    override def getJobInfos(limit: Int, status: Option[String]): Future[Seq[JobInfo]] =
      Future.successful(Seq())

    override def getJobInfosByContextId(
        contextId: String, jobStatuses: Option[Seq[String]] = None): Future[Seq[JobInfo]] = ???

    override def getJobInfo(jobId: String): Future[Option[JobInfo]] = ???

    override def saveJobInfo(jobInfo: JobInfo): Unit = ???

    override def getJobConfig(jobId: String): Future[Option[Config]] = ???

    override def getBinaryInfo(appName: String): Option[BinaryInfo] = ???

    override def deleteBinary(appName: String): Unit = {
      appName match {
        case "failOnThis" => throw new Exception("deliberate failure")
        case _ => //Do nothing
      }
    }

    override def cleanRunningJobInfosForContext(contextName: String, endTime: DateTime): Future[Unit] = {
      cleanupProbe.ref ! contextName
      Future.successful(())
    }
  }
}

class JobDAOActorSpec extends TestKit(JobDAOActorSpec.system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfterAll {

  import JobDAOActorSpec._

  val daoActor = system.actorOf(JobDAOActor.props(DummyDao))

  override def afterAll() {
    AkkaTestUtils.shutdownAndWait(system)
  }

  describe("JobDAOActor") {

    it("should respond when saving Binary completes successfully") {
      daoActor ! SaveBinary("succeed", BinaryType.Jar, DateTime.now, Array[Byte]())
      expectMsg(SaveBinaryResult(Success({})))
    }

    it("should respond when saving Binary fails") {
      daoActor ! SaveBinary("failOnThis", BinaryType.Jar, DateTime.now, Array[Byte]())
      expectMsgPF(3 seconds){
        case SaveBinaryResult(Failure(ex)) if ex.getMessage == "deliberate failure" =>
      }
    }

    it("should not block other calls to DAO if save binary is taking too long") {
      daoActor ! SaveBinary("blockDAO", BinaryType.Jar, DateTime.now, Array[Byte]())

      daoActor ! GetJobInfos(1)
      expectMsg(1.seconds, JobInfos(Seq()))

      daoActor ! SaveBinary("succeed", BinaryType.Jar, DateTime.now, Array[Byte]())
      expectMsg(1.seconds, SaveBinaryResult(Success({})))

      daoActor ! DeleteBinary("failOnThis")
      expectMsgPF(1.seconds){
        case DeleteBinaryResult(Failure(ex)) if ex.getMessage == "deliberate failure" =>
      }

      unblockingProbe.ref ! "unblock"
      expectMsg(4.seconds, SaveBinaryResult(Success({})))
    }

    it("should respond when deleting Binary completes successfully") {
      daoActor ! DeleteBinary("succeed")
      expectMsg(DeleteBinaryResult(Success({})))
    }

    it("should respond when deleting Binary fails") {
      daoActor ! DeleteBinary("failOnThis")
      expectMsgPF(3 seconds){
        case DeleteBinaryResult(Failure(ex)) if ex.getMessage == "deliberate failure" =>
      }
    }

    it("should return apps") {
      daoActor ! GetApps(None)
      expectMsg(Apps(Map(
        "app1" -> (BinaryType.Jar, dt),
        "app2" -> (BinaryType.Egg, dtplus1)
      )))
    }

    it("should get JobInfos") {
      daoActor ! GetJobInfos(1)
      expectMsg(JobInfos(Seq()))
    }

    it("should request jobs cleanup") {
      daoActor ! CleanContextJobInfos("context", DateTime.now())
      cleanupProbe.expectMsg("context")
    }

    it("should respond with successful message if dao operation was successful") {
      daoActor ! SaveContextInfo(ContextInfo("success", "name", "config", None,
        DateTime.now(), None, ContextStatus.Running, None))
      expectMsg(SavedSuccessfully)
    }

    it("should respond with failure message if dao operation has an exception") {
      daoActor ! SaveContextInfo(ContextInfo("failure", "name", "config", None,
        DateTime.now(), None, ContextStatus.Running, None))
      val failedMsg = expectMsgType[SaveFailed]
      failedMsg.error.getMessage should be("deliberate failure")
    }

    it("should update context by id with all attributes") {
      val contextId = "update-running"
      val endTime = DateTime.now()
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
          Some("new-address"), Some(endTime), ContextStatus.Error, Some(new Exception("Yay!"))))

      val msg = spyProbe.expectMsgType[ContextInfo]
      msg.id should be(contextId)
      msg.state should be(ContextStatus.Error)
      msg.actorAddress.get should be("new-address")
      msg.endTime.get should be(endTime)
      msg.error.get.getMessage should be("Yay!")
      unblockingProbe.ref ! "unblock"
      expectMsg(5.seconds, SavedSuccessfully)
    }

    it("should update with new values and if final state is being set then should also set the end time") {
      val contextId = "update-with-address"
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Killed, Some(new Exception("Nooo!"))))

      val msg = spyProbe.expectMsgType[ContextInfo]
      msg.id should be(contextId)
      msg.state should be(ContextStatus.Killed)
      msg.actorAddress.get should be("address")
      msg.endTime should not be(None)
      msg.error.get.getMessage should be("Nooo!")
      unblockingProbe.ref ! "unblock"
      expectMsg(5.seconds, SavedSuccessfully)
    }

    it("should update with new values and if non-final state is being set then endTime should be None") {
      val contextId = "update-non-final"
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Stopping))

      val msg = spyProbe.expectMsgType[ContextInfo]
      msg.id should be(contextId)
      msg.state should be(ContextStatus.Stopping)
      msg.actorAddress should be(None)
      msg.endTime should be(None)
      msg.error should be(None)
      unblockingProbe.ref ! "unblock"
      expectMsg(5.seconds, SavedSuccessfully)
    }

    it("should respond with SaveFailed if DAO calls fails (no context or exceptio)") {
      val contextId = "update-dao-fail"
      daoActor ! UpdateContextById(contextId, ContextInfoModifiable(
        ContextStatus.Restarting))
      expectMsgType[SaveFailed].error.getMessage() should be("deliberate failure")

      val contextId2 = "update-not-found"
      daoActor ! UpdateContextById(contextId2, ContextInfoModifiable(
        ContextStatus.Running))
      expectMsgType[SaveFailed].error.getMessage() should be(NoMatchingDAOObjectException().getMessage)

      val contextId3 = "update-fail"
      daoActor ! UpdateContextById(contextId3, ContextInfoModifiable(
        ContextStatus.Started))
      expectMsgType[SaveFailed].error.getMessage() should be("deliberate failure")
    }
  }
}
