package spark.jobserver.io

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.JobServer.InvalidConfiguration

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

abstract class CombinedDAOSpecBase {
  def config : Config
}

object CombinedDAOTestHelper {
  implicit val system: ActorSystem = ActorSystem("test")

  val binaryDAOBytesSuccess: Array[Byte] = "To test success BinaryDAO".toCharArray.map(_.toByte)
  val binaryDAOSuccessId: String = BinaryDAO.calculateBinaryHashString(binaryDAOBytesSuccess)
  val binaryDAOBytesFail: Array[Byte] = "To test failures BinaryDAO".toCharArray.map(_.toByte)
  val binaryDAOFailId: String = BinaryDAO.calculateBinaryHashString(binaryDAOBytesFail)
  val defaultDate: DateTime = DateTime.now()
  var testProbe: TestProbe = TestProbe()
}

class CombinedDAOSpec extends CombinedDAOSpecBase with FunSpecLike with BeforeAndAfterAll
  with Matchers{

    def config: Config = ConfigFactory.parseString(
      """
        |spark.jobserver.combineddao.rootdir = /tmp/spark-job-server-test/combineddao,
        |spark.jobserver.combineddao.binarydao.class = spark.jobserver.io.DummyBinaryDAO,
        |spark.jobserver.combineddao.metadatadao.class = spark.jobserver.io.DummyMetaDataDAO
      """.stripMargin
    )
    val daoTimeout: FiniteDuration = 3 seconds
    var dao: CombinedDAO = new CombinedDAO(config)

    override def beforeAll() {
      CombinedDAOTestHelper.testProbe = TestProbe()(ActorSystem("test"))
    }

    describe("check config validation in constructor") {
      it("should throw InvalidConfiguration if binaryDAO path is missing in config") {
        assertThrows[InvalidConfiguration] {
          new CombinedDAO (
            ConfigFactory.parseString(
              """
                |spark.jobserver.combineddao.rootdir = /tmp/spark-job-server-test/combineddao,
                |spark.jobserver.combineddao.metadatadao.class = spark.jobserver.io.DummyMetaDataDAO
              """.stripMargin
            )
          )
        }
      }

      it("should throw InvalidConfiguration if metadataDAO path is missing in config") {
        assertThrows[InvalidConfiguration] {
          new CombinedDAO (
            ConfigFactory.parseString(
              """
                |spark.jobserver.combineddao.rootdir = /tmp/spark-job-server-test/combineddao,
                |spark.jobserver.combineddao.binarydao.class = spark.jobserver.io.DummyBinaryDAO,
              """.stripMargin
            )
          )
        }
      }
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
        CombinedDAOTestHelper.testProbe.expectMsg("BinaryDAO: Save success")
        CombinedDAOTestHelper.testProbe.expectMsg("MetaDataDAO: Save success")
        CombinedDAOTestHelper.testProbe.expectNoMsg(daoTimeout)
      }

      it("should write no meta if binary file not saved") {
        dao.saveBinary("",
          BinaryType.Jar,
          CombinedDAOTestHelper.defaultDate,
          CombinedDAOTestHelper.binaryDAOBytesFail)
        CombinedDAOTestHelper.testProbe.expectMsg("BinaryDAO: Save failed")
        CombinedDAOTestHelper.testProbe.expectNoMsg(daoTimeout)
      }

      it("should try to delete binary if meta data save failed") {
        dao.saveBinary("failed",
          BinaryType.Jar,
          CombinedDAOTestHelper.defaultDate,
          CombinedDAOTestHelper.binaryDAOBytesSuccess)
        CombinedDAOTestHelper.testProbe.expectMsg("BinaryDAO: Save success")
        CombinedDAOTestHelper.testProbe.expectMsg("MetaDataDAO: Save failed")
        CombinedDAOTestHelper.testProbe.expectMsg("BinaryDAO: Delete success")
        CombinedDAOTestHelper.testProbe.expectNoMsg(daoTimeout)
      }

      it("should delete both meta and binary") {
        dao.deleteBinary("success")
        CombinedDAOTestHelper.testProbe.expectMsg("MetaDataDAO: getBinary success")
        CombinedDAOTestHelper.testProbe.expectMsg("MetaDataDAO: Delete success")
        CombinedDAOTestHelper.testProbe.expectMsg("BinaryDAO: Delete success")
        CombinedDAOTestHelper.testProbe.expectNoMsg(daoTimeout)
      }

      it("should not delete binary if meta is not deleted") {
        dao.deleteBinary("get-info-success-del-info-failed")
        CombinedDAOTestHelper.testProbe.expectMsg("MetaDataDAO: getBinary success")
        CombinedDAOTestHelper.testProbe.expectMsg("MetaDataDAO: Delete failed")
        CombinedDAOTestHelper.testProbe.expectNoMsg(daoTimeout)
      }

      it("should do nothing if get info failed") {
        dao.deleteBinary("get-info-failed")
        CombinedDAOTestHelper.testProbe.expectMsg("MetaDataDAO: getBinary failed")
        CombinedDAOTestHelper.testProbe.expectNoMsg(daoTimeout)
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

class DummyBinaryDAO(config: Config) extends BinaryDAO {
  override def save(id: String, binaryBytes: Array[Byte]): Future[Boolean] = {
    id match {
      case CombinedDAOTestHelper.`binaryDAOSuccessId` =>
        CombinedDAOTestHelper.testProbe.ref ! "BinaryDAO: Save success"
        Future.successful(true)
      case CombinedDAOTestHelper.`binaryDAOFailId` =>
        CombinedDAOTestHelper.testProbe.ref ! "BinaryDAO: Save failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def delete(id: String): Future[Boolean] = {
    id match {
      case CombinedDAOTestHelper.`binaryDAOSuccessId` =>
        CombinedDAOTestHelper.testProbe.ref ! "BinaryDAO: Delete success"
        Future.successful(true)
      case CombinedDAOTestHelper.`binaryDAOFailId` =>
        CombinedDAOTestHelper.testProbe.ref ! "BinaryDAO: Delete failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def get(id: String): Future[Option[Array[Byte]]] = {
    CombinedDAOTestHelper.testProbe.ref ! "BinaryDAO: Get success"
    Future.successful(Some(CombinedDAOTestHelper.binaryDAOBytesSuccess))
  }
}

class DummyMetaDataDAO(config: Config) extends MetaDataDAO {
  override def saveBinary(name: String,
                          binaryType: BinaryType,
                          uploadTime: DateTime,
                          id: String): Future[Boolean] = {
    name match {
      case message if message.contains("save-info-success") || message == "success" =>
        CombinedDAOTestHelper.testProbe.ref ! "MetaDataDAO: Save success"
        Future.successful(true)
      case message if message.contains("save-info-failed") || message == "failed" =>
        CombinedDAOTestHelper.testProbe.ref ! "MetaDataDAO: Save failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def deleteBinary(name: String): Future[Boolean] = {
    name match {
      case message if message.contains("del-info-success") || message == "success" =>
        CombinedDAOTestHelper.testProbe.ref ! "MetaDataDAO: Delete success"
        Future.successful(true)
      case message if message.contains("del-info-failed") || message == "failed" =>
        CombinedDAOTestHelper.testProbe.ref ! "MetaDataDAO: Delete failed"
        Future.successful(false)
      case _ => Future.successful(false)
    }
  }

  override def getBinary(name: String): Future[Option[BinaryInfo]] = {
    name match {
      case message if message.contains("get-info-success") || message == "success" =>
        CombinedDAOTestHelper.testProbe.ref ! "MetaDataDAO: getBinary success"
        Future.successful(Some(
          BinaryInfo("success", BinaryType.Jar, DateTime.now(), CombinedDAOTestHelper.binaryDAOSuccessId))
        )
      case _ =>
        CombinedDAOTestHelper.testProbe.ref ! "MetaDataDAO: getBinary failed"
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
