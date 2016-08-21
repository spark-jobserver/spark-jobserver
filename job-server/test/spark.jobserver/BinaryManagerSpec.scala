package spark.jobserver

import akka.actor.{Props, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import ooyala.common.akka.InstrumentedActor
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSpecLike}
import spark.jobserver.io.BinaryType

import scala.util.{Success, Failure}
import scala.concurrent.duration._

object BinaryManagerSpec {
  val system = ActorSystem("binary-manager-test")

  val dt = DateTime.now

  class DummyDAOActor extends InstrumentedActor {

    import spark.jobserver.io.JobDAOActor._

    override def wrappedReceive: Receive = {
      case GetApps(_) =>
        sender ! Apps(Map("app1" -> (BinaryType.Jar, dt)))
      case SaveBinary("failOnThis", _, _, _) =>
        sender ! SaveBinaryResult(Failure(new Exception("deliberate failure")))
      case SaveBinary(_, _, _, _) =>
        sender ! SaveBinaryResult(Success({}))
    }
  }
}

class BinaryManagerSpec extends TestKit(BinaryManagerSpec.system) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfterAll {

  import spark.jobserver.BinaryManagerSpec._

  override def afterAll() {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(system)
  }

  val daoActor = system.actorOf(Props[DummyDAOActor])
  val binaryManager = system.actorOf(Props(classOf[BinaryManager], daoActor))

  describe("BinaryManager") {

    it("should list binaries") {
      binaryManager ! ListBinaries(None)
      expectMsg(Map("app1" -> (BinaryType.Jar, dt)))
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
  }
}
