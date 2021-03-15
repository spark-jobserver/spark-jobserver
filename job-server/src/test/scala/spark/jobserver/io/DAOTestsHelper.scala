package spark.jobserver.io

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.Config

import java.time.ZonedDateTime
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object DAOTestsHelper {
  implicit val system: ActorSystem = ActorSystem("test")

  val binaryDAOBytesSuccess: Array[Byte] = "To test success BinaryDAO".toCharArray.map(_.toByte)
  val binaryDAOSuccessId: String = BinaryDAO.calculateBinaryHashString(binaryDAOBytesSuccess)
  val binaryDAOBytesFail: Array[Byte] = "To test failures BinaryDAO".toCharArray.map(_.toByte)
  val binaryDAOFailId: String = BinaryDAO.calculateBinaryHashString(binaryDAOBytesFail)
  val defaultDate: ZonedDateTime = ZonedDateTime.now()
  val someBinaryName: String = "name-del-info-success"
  val someBinaryId = BinaryDAO.calculateBinaryHashString(Array(10, 11, 12))
  val someBinaryInfo: BinaryInfo = BinaryInfo(someBinaryName, BinaryType.Jar, defaultDate, Some(someBinaryId))
  val someOtherBinaryBytes: Array[Byte] = Array(7, 8, 9)
  val someOtherBinaryId: String = BinaryDAO.calculateBinaryHashString(someOtherBinaryBytes)
  val someOtherBinaryName: String = "other-name-del-info-success"
  val someOtherBinaryInfo: BinaryInfo = BinaryInfo(someOtherBinaryName, BinaryType.Jar, defaultDate,
      Some(someOtherBinaryId))
  var testProbe: TestProbe = TestProbe()
}

class DummyBinaryDAO(config: Config) extends BinaryDAO {
  override def save(id: String, binaryBytes: Array[Byte]): Future[Boolean] = {
    id match {
      case DAOTestsHelper.`binaryDAOSuccessId` =>
        DAOTestsHelper.testProbe.ref ! "BinaryDAO: Save success"
        Future.successful(true)
      case DAOTestsHelper.`binaryDAOFailId` =>
        DAOTestsHelper.testProbe.ref ! "BinaryDAO: Save failed"
        Future.successful(false)
      case _ =>
        DAOTestsHelper.testProbe.ref ! "BinaryDAO: unexpected id " + id + " in save"
        Future.successful(false)
    }
  }

  override def delete(id: String): Future[Boolean] = {
    id match {
      case DAOTestsHelper.`binaryDAOSuccessId` =>
        DAOTestsHelper.testProbe.ref ! "BinaryDAO: Delete success"
        Future.successful(true)
      case DAOTestsHelper.`binaryDAOFailId` =>
        DAOTestsHelper.testProbe.ref ! "BinaryDAO: Delete failed"
        Future.successful(false)
      case DAOTestsHelper.someOtherBinaryId =>
        DAOTestsHelper.testProbe.ref ! "BinaryDAO: Delete success"
        Future.successful(true)
      case _ =>
        DAOTestsHelper.testProbe.ref ! "BinaryDAO: unexpected id " + id + " in delete"
        Future.successful(false)
    }
  }

  override def get(id: String): Future[Option[Array[Byte]]] = {
    DAOTestsHelper.testProbe.ref ! "BinaryDAO: Get success"
    Future.successful(Some(DAOTestsHelper.binaryDAOBytesSuccess))
  }
}

class DummyMetaDataDAO(config: Config) extends MetaDataDAO {
  val jobConfigs = mutable.HashMap.empty[String, Config]
  val contextInfos = mutable.HashMap.empty[String, ContextInfo]
  val jobInfos = mutable.HashMap.empty[String, JobInfo]

  override def saveBinary(name: String,
                          binaryType: BinaryType,
                          uploadTime: ZonedDateTime,
                          id: String): Future[Boolean] = {
    name match {
      case message if message.contains("save-info-success") || message == "success" =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: Save success"
        Future.successful(true)
      case message if message.startsWith("long-call") =>
        val ms = message.split("-").last.toInt
        Thread.sleep(ms)
        Future.successful(true)
      case message if message.contains("save-info-failed") || message == "failed" =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: Save failed"
        Future.successful(false)
      case _ =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: unexpected name " + name + " in save"
        Future.successful(false)
    }
  }

  override def deleteBinary(name: String): Future[Boolean] = {
    name match {
      case message if message.contains("del-info-success") || message == "success" =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: Delete success"
        Future.successful(true)
      case message if message.contains("del-info-failed") || message == "failed" =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: Delete failed"
        Future.successful(false)
      case _ =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: unexpected name " + name + " in delete"
        Future.successful(false)
    }
  }

  override def getBinary(name: String): Future[Option[BinaryInfo]] = {
    name match {
      case DAOTestsHelper.someBinaryName =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinary success"
        Future.successful(Some(
            DAOTestsHelper.someBinaryInfo
          )
        )
      case DAOTestsHelper.someOtherBinaryName =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinary success"
        Future.successful(Some(
            DAOTestsHelper.someOtherBinaryInfo
          )
        )
      case message if message.contains("get-info-success") || message == "success" =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinary success"
        Future.successful(Some(
          BinaryInfo("success", BinaryType.Jar, ZonedDateTime.now(), Some(DAOTestsHelper.binaryDAOSuccessId)))
        )
      case _ =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinary failed"
        Future.successful(None)
    }
  }

  override def getBinaries: Future[Seq[BinaryInfo]] = {
    Future.successful(
      Seq(
        DAOTestsHelper.someBinaryInfo,
        DAOTestsHelper.someOtherBinaryInfo,
        BinaryInfo("name3", BinaryType.Jar, DAOTestsHelper.defaultDate),
        BinaryInfo("name4", BinaryType.Jar, DAOTestsHelper.defaultDate),
        BinaryInfo("name5", BinaryType.Jar, DAOTestsHelper.defaultDate)
      )
    )
  }

  override def getBinariesByStorageId(storageId: String): Future[Seq[BinaryInfo]] = {
    storageId match {
      case DAOTestsHelper.someBinaryId =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinariesByStorageId success"
        Future.successful(Seq(
          DAOTestsHelper.someBinaryInfo,
          BinaryInfo("someName", BinaryType.Jar, DAOTestsHelper.defaultDate,
              Some(DAOTestsHelper.someBinaryId)))
        )
      case DAOTestsHelper.someOtherBinaryId =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinariesByStorageId success"
        Future.successful(Seq(
          DAOTestsHelper.someOtherBinaryInfo
        )
      )
      case message if message.contains("get-info-success") || message == "success" =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinariesByStorageId success yes"
        Future.successful(Seq(
          BinaryInfo("success", BinaryType.Jar, ZonedDateTime.now(), Some(DAOTestsHelper.binaryDAOSuccessId)))
        )
      case _ =>
        DAOTestsHelper.testProbe.ref ! "MetaDataDAO: getBinariesByStorageId success"
        Future.successful(Seq())
    }
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = {
    Future.successful(jobConfigs.get(jobId))
  }

  override def saveJobConfig(id: String, config: Config): Future[Boolean] = {
    id match {
      case "job-config-id" =>
        Future {
          Thread.sleep(1000) // mimic long save operation
          jobConfigs(id) = config
          true
        }
      case "job-config-fail" => Future.successful(false)
    }
  }

  override def getJobsByContextId(contextId: String, statuses: Option[Seq[String]]): Future[Seq[JobInfo]] = {
    Future {
      val jobsAgainstContextId = jobInfos.filter(_._2.contextId == contextId)
      val filteredJobs = statuses match {
        case None => jobsAgainstContextId
        case Some(status) => jobsAgainstContextId.filter(j => status.contains(j._2.state))
      }
      filteredJobs.map(_._2).toSeq
    }
  }

  override def getJobs(limit: Int, status: Option[String]): Future[Seq[JobInfo]] = Future {
    limit match {
      case 1 =>
        val jobInfo =
          JobInfo("bar", "cid", "context",
            "com.abc.meme", JobStatus.Running, DAOTestsHelper.defaultDate, None,
            None, Seq(BinaryInfo("demo", BinaryType.Egg, DAOTestsHelper.defaultDate)))
        Seq(jobInfo)
      case 0 => Seq()
    }
  }

  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
    Future[Seq[JobInfo]] = Future {
      binName match {
        case "multiple" =>
          val jobInfo =
            JobInfo("bar", "cid", "context",
              "com.abc.meme", JobStatus.Running, DAOTestsHelper.defaultDate, None,
              None, Seq(BinaryInfo("demo", BinaryType.Egg, DAOTestsHelper.defaultDate)))
          Seq(jobInfo, jobInfo.copy(jobId = "kaboom"))
        case _ => Seq()
      }
  }

  override def getJob(id: String): Future[Option[JobInfo]] = {
    Future.successful(jobInfos.get(id))
  }

  override def saveJob(jobInfo: JobInfo): Future[Boolean] = {
    jobInfo.jobId match {
      case "jid" | "dummy" =>
        Future {
          Thread.sleep(1000) // mimic long save operation
          jobInfos(jobInfo.jobId) = jobInfo
          true
        }
      case "jid-fail" => Future.successful(false)
    }
  }

  override def getContexts(limit: Option[Int], statuses: Option[Seq[String]]): Future[Seq[ContextInfo]] = ???

  override def getContextByName(name: String): Future[Option[ContextInfo]] = ???

  override def getContext(id: String): Future[Option[ContextInfo]] = {
    Future.successful(contextInfos.get(id))
  }

  override def saveContext(contextInfo: ContextInfo): Future[Boolean] = {
    contextInfo.id match {
      case "success" =>
        Future {
          Thread.sleep(100)
          contextInfos(contextInfo.id) = contextInfo
          true
        }
      case "cid-fail" | "failure" => Future.successful(false)
      case _ =>
        Future {
          Thread.sleep(1000) // mimic long save operation
          contextInfos(contextInfo.id) = contextInfo
          true
        }
    }
  }
}
