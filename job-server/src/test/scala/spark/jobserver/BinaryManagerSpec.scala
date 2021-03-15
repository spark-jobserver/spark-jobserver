package spark.jobserver

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.common.akka.{AkkaTestUtils, InstrumentedActor}
import spark.jobserver.io.{BinaryInfo, BinaryType, JobInfo, JobStatus}
import spark.jobserver.io.JobDAOActor.LastBinaryInfo

import java.time.ZonedDateTime

object BinaryManagerSpec {
  val system = ActorSystem("binary-manager-test")

  val dt = ZonedDateTime.now
  val binaryInfo = BinaryInfo("binary", BinaryType.Jar, dt, None)

  val binInfo = BinaryInfo("demo", BinaryType.Egg, ZonedDateTime.now)

  class DummyDAOActor extends InstrumentedActor {

    import spark.jobserver.io.JobDAOActor._

    val jobInfo = JobInfo("bar", "cid", "context",
        "com.abc.meme", JobStatus.Running, ZonedDateTime.now, None, None, Seq(binInfo), None)

    override def wrappedReceive: Receive = {
      case GetLastBinaryInfo("binary") =>
        sender ! LastBinaryInfo(Some(binaryInfo))
      case GetLastBinaryInfo("nonexistingbinary") =>
        sender ! LastBinaryInfo(None)
      case GetApps(_) =>
        sender ! Apps(Map("app1" -> (BinaryType.Jar, dt)))
      case SaveBinary("failOnThis", _, _, _) =>
        sender ! SaveBinaryResult(Failure(new Exception("deliberate failure")))
      case SaveBinary(_, _, _, _) =>
        sender ! SaveBinaryResult(Success({}))
      case DeleteBinary(_) =>
        sender ! DeleteBinaryResult(Success({}))
      case GetJobsByBinaryName(appName, statuses) =>
        appName match {
          case "empty" => sender ! JobInfos(Seq())
          case "running" => sender ! JobInfos(Seq(jobInfo))
          case "fail" =>
        }
      case GetBinaryInfosForCp(classPath) =>
        classPath match {
          case Seq("success") => sender ! BinaryInfosForCp(Seq(binInfo))
          case Seq("notfound") => sender ! BinaryNotFound("notfound")
          case Seq("failure") => sender ! GetBinaryInfosForCpFailed(new Exception("failure"))
        }
    }
  }
}

class BinaryManagerSpec extends TestKit(BinaryManagerSpec.system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfterAll {

  import spark.jobserver.BinaryManagerSpec._

  override def afterAll() {
    AkkaTestUtils.shutdownAndWait(system)
  }

  val daoActor = system.actorOf(Props[DummyDAOActor])
  val binaryManager = system.actorOf(Props(classOf[BinaryManager], daoActor))

  describe("BinaryManager") {

    it("should list binaries") {
      binaryManager ! ListBinaries(None)
      expectMsg(Map("app1" -> (BinaryType.Jar, dt)))
    }

    it("should return a binary by name"){
      binaryManager ! GetBinary("binary")
      expectMsg(LastBinaryInfo(Some(binaryInfo)))
    }

    it("should return a proper response when queried for a non-existing binary"){
      binaryManager ! GetBinary("nonexistingbinary")
      expectMsg(LastBinaryInfo(None))
    }

    it("should respond when binary is saved successfully") {
      binaryManager ! StoreBinary("valid", BinaryType.Jar, Array[Byte](0x50, 0x4b, 0x03, 0x04, 0x05))
      expectMsg(BinaryStored)
    }

    it("should respond when binary is invalid") {
      binaryManager ! StoreBinary("invalid", BinaryType.Jar, Array[Byte](0x51, 0x4b, 0x03, 0x04, 0x05))
      expectMsg(InvalidBinary)
    }

    it("should respond when underlying DAO fails to store") {
      binaryManager ! StoreBinary("failOnThis", BinaryType.Jar, Array[Byte](0x50, 0x4b, 0x03, 0x04, 0x05))
      expectMsgPF(3 seconds){case BinaryStorageFailure(ex) if ex.getMessage == "deliberate failure" => }
    }

    it("should respond when deleted successfully if no active job is using the binary") {
      binaryManager ! DeleteBinary("empty")
      expectMsg(3.seconds, BinaryDeleted)
    }

    it("should not delete if binary is still in use") {
      binaryManager ! DeleteBinary("running")
      expectMsg(3.seconds, BinaryInUse(Seq("bar")))
    }

    it("should handle failures during deletion of binary and within timeout") {
      binaryManager ! DeleteBinary("fail")
      expectMsgType[BinaryDeletionFailure](BinaryManager.DELETE_TIMEOUT + 1.seconds)
    }

    it("should return list of binary infos for class path") {
      binaryManager ! GetBinaryInfoListForCp(Seq("success"))
      expectMsg(3.seconds, BinaryInfoListForCp(Seq(binInfo)))
    }

    it("should return an error if binary not found") {
      binaryManager ! GetBinaryInfoListForCp(Seq("notfound"))
      expectMsg(3.seconds, NoSuchBinary("notfound"))
    }

    it("should return an error if unexpected event happend") {
      binaryManager ! GetBinaryInfoListForCp(Seq("failure"))
      expectMsgType[GetBinaryInfoListForCpFailure]
    }
  }
}
