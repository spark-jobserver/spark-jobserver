package spark.jobserver.io

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

abstract class CombinedDAOSpecBase {
  def config : Config
}

object CombinedDAOTestHelper {
  val binaryDAOBytesSuccess: Array[Byte] = Array(1, 2, 3)
  val binaryDAOSuccessId: String = BinaryDAO.calculateBinaryHashString(binaryDAOBytesSuccess)
  val binaryDAOBytesFail: Array[Byte] = Array(4, 5, 6)
  val binaryDAOFailId: String = BinaryDAO.calculateBinaryHashString(binaryDAOBytesFail)
  val defaultDate: DateTime = DateTime.now()
}

class CombinedDAOSpec extends CombinedDAOSpecBase with FunSpecLike with BeforeAndAfterAll
  with Matchers{

    def config: Config = ConfigFactory.parseString(
      "spark.jobserver.combineddao.rootdir = /tmp/spark-job-server-test/combineddao"
    )
    val dao_timeout = 3 seconds
    var testProbe: TestProbe = _
    var dao: CombinedDAO = _
    implicit val system: ActorSystem = ActorSystem("test")

    override def beforeAll() {
      testProbe = TestProbe()
      dao = new CombinedDAO(config,
        new DummyBinaryDAO(testProbe.ref),
        new DummyMetaDataDAO(testProbe.ref)
      )
    }

    describe("binary hash conversions") {
      it("should convert byte hash to string and back") {
        val hash = BinaryDAO.calculateBinaryHash(
          Array(1, 4, -1, 7): Array[Byte]
        )
        hash should equal (BinaryDAO.hashStringToBytes(
          BinaryDAO.hashBytesToString(hash)
        ))
      }

      it("calculate hash string equals calculate byte hash and convert it") {
        val testBytes : Array[Byte] = Array(1, 4, -1, 7)
        BinaryDAO.calculateBinaryHashString(testBytes) should equal(
          BinaryDAO.hashBytesToString(BinaryDAO.calculateBinaryHash(testBytes))
        )
      }

      it("should create correct hex hash string") {
        val testBytes = "Test string".toCharArray.map(_.toByte)
        BinaryDAO.calculateBinaryHashString(testBytes) should
          equal("a3e49d843df13c2e2a7786f6ecd7e0d184f45d718d1ac1a8a63e570466e489dd")
      }

      it("taking hash string equals taking byte hash and converting to string") {
        val testBytes = "Test string".toCharArray.map(_.toByte)
        BinaryDAO.calculateBinaryHashString(testBytes) should equal (
          BinaryDAO.hashBytesToString(BinaryDAO.calculateBinaryHash(testBytes))
        )
      }
    }

    describe("save, get and delete a binary file") {
      it("should be able to save one binary file") {
        dao.saveBinary("success",
          BinaryType.Jar,
          CombinedDAOTestHelper.defaultDate,
          CombinedDAOTestHelper.binaryDAOBytesSuccess)
        testProbe.expectMsg("BinaryDAO: Save success")
        testProbe.expectMsg("MetaDataDAO: Save success")
      }

      it("should write no meta if binary file not saved") {
        dao.saveBinary("",
          BinaryType.Jar,
          CombinedDAOTestHelper.defaultDate,
          CombinedDAOTestHelper.binaryDAOBytesFail)
        testProbe.expectMsg("BinaryDAO: Save failed")
        testProbe.expectNoMsg(dao_timeout)
      }

      it("should try to delete binary if meta data save failed") {
        dao.saveBinary("failed",
          BinaryType.Jar,
          CombinedDAOTestHelper.defaultDate,
          CombinedDAOTestHelper.binaryDAOBytesSuccess)
        testProbe.expectMsg("BinaryDAO: Save success")
        testProbe.expectMsg("MetaDataDAO: Save failed")
        testProbe.expectMsg("BinaryDAO: Delete success")
        testProbe.expectNoMsg(dao_timeout)
      }

      it("should delete both meta and binary") {
        dao.deleteBinary("success")
        testProbe.expectMsg("MetaDataDAO: getBinary success")
        testProbe.expectMsg("MetaDataDAO: Delete success")
        testProbe.expectMsg("BinaryDAO: Delete success")
        testProbe.expectNoMsg(dao_timeout)
      }

      it("should not delete binary if meta is not deleted") {
        dao.deleteBinary("get-info-success-del-info-failed")
        testProbe.expectMsg("MetaDataDAO: getBinary success")
        testProbe.expectMsg("MetaDataDAO: Delete failed")
        testProbe.expectNoMsg(dao_timeout)
      }

      it("should do nothing if get info failed") {
        dao.deleteBinary("get-info-failed")
        testProbe.expectMsg("MetaDataDAO: getBinary failed")
        testProbe.expectNoMsg(dao_timeout)
      }
    }

    describe("get information about binaries") {
      it("should get info for all binaries") {
        val names = Await.result(dao.getApps, 60 seconds)
        names should equal (Map(
          "name1" -> (BinaryType.Jar, CombinedDAOTestHelper.defaultDate),
          "name2" -> (BinaryType.Jar, CombinedDAOTestHelper.defaultDate),
          "name3" -> (BinaryType.Jar, CombinedDAOTestHelper.defaultDate),
          "name4" -> (BinaryType.Jar, CombinedDAOTestHelper.defaultDate),
          "name5" -> (BinaryType.Jar, CombinedDAOTestHelper.defaultDate)
        ))
      }

      it("should see if binary hash is referenced by several metas") {
        Await.result(dao.isBinaryUsed("hash"), 10 seconds) should equal (true)
      }

      it("should see if binary hash is not referenced by other meta except for given one") {
        Await.result(dao.isBinaryUsed("one_more_hash", "name2"), 10 seconds) should equal (false)
      }

      it("should see if binary hash is referenced under name except for given one") {
        Await.result(dao.isBinaryUsed("hash", "name1"), 10 seconds) should equal (true)
      }

      it("should see if binary hash is referenced by any meta") {
        Await.result(dao.isBinaryUsed("one_more_hash"), 10 seconds)  should equal (true)
      }
    }
}

class DummyBinaryDAO(testProbeRef: ActorRef) extends BinaryDAO {
  override def validateConfig(config: Config): Boolean = true

  override def save(id: String, binaryBytes: Array[Byte]): Future[Boolean] = {
    id match {
      case CombinedDAOTestHelper.`binaryDAOSuccessId` =>
        testProbeRef ! "BinaryDAO: Save success"
        Future.successful(true)
      case CombinedDAOTestHelper.`binaryDAOFailId` =>
        testProbeRef ! "BinaryDAO: Save failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def delete(id: String): Future[Boolean] = {
    id match {
      case CombinedDAOTestHelper.`binaryDAOSuccessId` =>
        testProbeRef ! "BinaryDAO: Delete success"
        Future.successful(true)
      case CombinedDAOTestHelper.`binaryDAOFailId` =>
        testProbeRef ! "BinaryDAO: Delete failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def get(id: String): Future[Option[Array[Byte]]] = {
    testProbeRef ! "BinaryDAO: Get success"
    Future.successful(Some(CombinedDAOTestHelper.binaryDAOBytesSuccess))
  }
}

class DummyMetaDataDAO(testProbeRef: ActorRef) extends MetaDataDAO {
  override def saveBinary(name: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          id: String): Future[Boolean] = {
    name match {
      case message if message.contains("save-info-success") || message == "success" =>
        testProbeRef ! "MetaDataDAO: Save success"
        Future.successful(true)
      case message if message.contains("save-info-failed") || message == "failed" =>
        testProbeRef ! "MetaDataDAO: Save failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def deleteBinary(name: String): Future[Boolean] = {
    name match {
      case message if message.contains("del-info-success") || message == "success" =>
        testProbeRef ! "MetaDataDAO: Delete success"
        Future.successful(true)
      case message if message.contains("del-info-failed") || message == "failed" =>
        testProbeRef ! "MetaDataDAO: Delete failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def getBinary(name: String): Future[Option[BinaryInfo]] = {
    name match {
      case message if message.contains("get-info-success") || message == "success" =>
        testProbeRef ! "MetaDataDAO: getBinary success"
        Future.successful(Some(
          BinaryInfo("success", BinaryType.Jar, DateTime.now(), CombinedDAOTestHelper.binaryDAOSuccessId))
        )
      case _ =>
        testProbeRef ! "MetaDataDAO: getBinary failed"
        Future.successful(None)
    }
  }

  override def getBinaries: Future[Seq[BinaryInfo]] = {
    Future.successful(
      Seq(
        BinaryInfo("name1", BinaryType.Jar, CombinedDAOTestHelper.defaultDate, "hash"),
        BinaryInfo("name2", BinaryType.Jar, CombinedDAOTestHelper.defaultDate, "one_more_hash"),
        BinaryInfo("name3", BinaryType.Jar, CombinedDAOTestHelper.defaultDate, "another_hash"),
        BinaryInfo("name4", BinaryType.Jar, CombinedDAOTestHelper.defaultDate, "more_hashes!"),
        BinaryInfo("name5", BinaryType.Jar, CombinedDAOTestHelper.defaultDate, "hash")
      )
    )
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = ???

  override def saveJobConfig(id: String, config: Config): Future[Boolean] = ???

  override def getJobsByContextId(contextId: String, statuses: Option[Seq[String]]): Future[Seq[JobInfo]] = ???

  override def getJobs(limit: Int, status: Option[String]): Future[Seq[JobInfo]] = ???

  override def getJob(id: String): Future[Option[JobInfo]] = ???

  override def saveJob(jobInfo: JobInfo): Future[Boolean] = ???

  override def getContexts(limit: Option[Int], statuses: Option[Seq[String]]): Future[Seq[ContextInfo]] = ???

  override def getContextByName(name: String): Future[Option[ContextInfo]] = ???

  override def getContext(id: String): Future[Option[ContextInfo]] = ???

  override def saveContext(contextInfo: ContextInfo): Future[Boolean] = ???
}
