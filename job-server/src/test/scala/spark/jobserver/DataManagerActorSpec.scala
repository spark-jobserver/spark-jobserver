package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import java.nio.file.{Files, Paths}

import spark.jobserver.common.akka
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.DataFileDAO
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

object DataManagerActorSpec {
  val system = ActorSystem("test")
}

class DataManagerActorSpec extends TestKit(DataManagerActorSpec.system) with ImplicitSender
    with AnyFunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import com.typesafe.config._
  import DataManagerActor._

  private val bytes = Array[Byte](0, 1, 2)
  private val tmpDir = Files.createTempDirectory("ut")
  private val config = ConfigFactory.empty().withValue("spark.jobserver.datadao.rootdir",
    ConfigValueFactory.fromAnyRef(tmpDir.toString))

  override def afterAll() {
    dao.shutdown()
    AkkaTestUtils.shutdownAndWait(actor)
    AkkaTestUtils.shutdownAndWait(brokenDaoActor)
    akka.AkkaTestUtils.shutdownAndWait(DataManagerActorSpec.system)
    Files.deleteIfExists(tmpDir.resolve(DataFileDAO.META_DATA_FILE_NAME))
    Files.delete(tmpDir)
  }

  val dao: DataFileDAO = new DataFileDAO(config)
  val actor: ActorRef = system.actorOf(DataManagerActor.props(dao), "data-manager")

  val brokenDAO: DataFileDAO = new DataFileDAO(config) {
    override def deleteAll(): Boolean = false
  }
  val brokenDaoActor: ActorRef = system.actorOf(DataManagerActor.props(brokenDAO), "broken-dao-data-manager")

  describe("DataManagerActor") {
    it("should store, list and delete tmp data file") {
      val fileName = System.currentTimeMillis + "tmpFile"

      actor ! StoreData(fileName, bytes)
      val fn = expectMsgPF() {
        case Stored(msg) => msg
      }

      fn.contains(fileName) should be(true)
      dao.listFiles.exists(f => f.contains(fileName)) should be(true)
      actor ! DeleteData(fn)
      expectMsg(Deleted)
      dao.listFiles.exists(f => f.contains(fileName)) should be(false)
    }

    it("should list data files") {
      actor ! ListData

      val storedFiles = expectMsgPF() {
        case files => files
      }

      storedFiles should equal(dao.listFiles)
    }

    it("should store, list and delete several files") {
      val storedFiles = (for (ix <- 1 to 11; fileName = System.currentTimeMillis + "tmpFile" + ix) yield {
        actor ! StoreData(fileName, bytes)
        expectMsgPF() {
          case Stored(msg) => msg
        }
      }).toSet

      dao.listFiles should equal(storedFiles)
      storedFiles foreach (fn => {
        actor ! DeleteData(fn)
        expectMsg(Deleted)
      })
      dao.listFiles should equal(Set())
    }

    it("should store and delete all files") {
      actor ! StoreData("test-file-1", bytes)
      val firstFileName = expectMsgPF() { case Stored(name) => name }
      val firstFile = Paths.get(firstFileName)
      Files.exists(firstFile) should be (true)

      actor ! StoreData("test-file-2", bytes)
      val secondFileName = expectMsgPF() { case Stored(name) => name }
      val secondFile = Paths.get(secondFileName)
      Files.exists(secondFile) should be (true)

      actor ! DeleteAllData
      expectMsg(Deleted)
      Files.exists(firstFile) should be (false)
      Files.exists(secondFile) should be (false)
    }

    it("should provide files and notify job manager on delete") {
      val fileName = System.currentTimeMillis + "tmpFile"

      actor ! StoreData(fileName, bytes)
      val storedFileName = expectMsgPF() {
        case Stored(name) => name
      }

      val dummyJobManager = TestProbe()
      actor ! RetrieveData(storedFileName, dummyJobManager.ref)
      val retrievedData = expectMsgPF() {
        case Data(data) => data
      }
      retrievedData should equal (bytes)

      actor ! DeleteData(storedFileName)
      expectMsg(Deleted)
      dummyJobManager.expectMsg(JobManagerActor.DeleteData(storedFileName))
    }

    it("removes remote cached files on delete all") {
      val fileName = System.currentTimeMillis + "tmpFile"

      actor ! StoreData(fileName, bytes)
      val storedFileName = expectMsgPF() {
        case Stored(name) => name
      }

      val dummyJobManager = TestProbe()
      actor ! RetrieveData(storedFileName, dummyJobManager.ref)
      val retrievedData = expectMsgPF() {
        case Data(data) => data
      }
      retrievedData should equal (bytes)

      actor ! DeleteAllData
      expectMsg(Deleted)
      dummyJobManager.expectMsg(JobManagerActor.DeleteData(storedFileName))
    }

    it("should fail to read unknown files") {
      val dummyJobManager = TestProbe()
      actor ! RetrieveData("unknown-file", dummyJobManager.ref)
      expectMsgClass(classOf[Error])
    }

    it("should fail to delete unknown files") {
      actor ! DeleteData("unknown-file")
      expectMsgClass(classOf[Error])
    }

    it("should handle errors in delete all") {
      brokenDaoActor ! DeleteAllData
      expectMsgClass(classOf[Error])
    }
  }
}
