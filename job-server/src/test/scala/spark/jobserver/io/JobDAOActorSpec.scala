package spark.jobserver.io

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.Config
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.JobDAOActor._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import spark.jobserver.common.akka.AkkaTestUtils

object JobDAOActorSpec {
  val system = ActorSystem("dao-test")
  val dt = DateTime.now()
  val dtplus1 = dt.plusHours(1)

  val cleanupProbe = TestProbe()(system)

  object DummyDao extends JobDAO{

    override def saveBinary(appName: String, binaryType: BinaryType,
                            uploadTime: DateTime, binaryBytes: Array[Byte]): Unit = {
      appName match {
        case "failOnThis" => throw new Exception("deliberate failure")
        case _ => //Do nothing
      }
    }

    override def getApps: Future[Map[String, (BinaryType, DateTime)]] =
      Future.successful(Map(
        "app1" -> (BinaryType.Jar, dt),
        "app2" -> (BinaryType.Egg, dtplus1)
      ))

    override def retrieveBinaryFile(appName: String,
                                    binaryType: BinaryType, uploadTime: DateTime): String = ???

    override def saveContextInfo(contextInfo: ContextInfo): Unit = ???

    override def getContextInfo(id: String): Future[Option[ContextInfo]] = ???

    override def getContextInfos(limit: Option[Int] = None, statusOpt: Option[String] = None):
      Future[Seq[ContextInfo]] = ???

    override def getContextInfoByName(name: String): Future[Option[ContextInfo]] = ???

    override def saveJobConfig(jobId: String, jobConfig: Config): Unit = ???

    override def getJobInfos(limit: Int, status: Option[String]): Future[Seq[JobInfo]] =
      Future.successful(Seq())

    override def getRunningJobInfosForContextName(contextName: String): Future[Seq[JobInfo]] = ???

    override def getJobInfo(jobId: String): Future[Option[JobInfo]] = ???

    override def saveJobInfo(jobInfo: JobInfo): Unit = ???

    override def getJobConfig(jobId: String): Future[Option[Config]] = ???

    override def getLastUploadTimeAndType(appName: String): Option[(DateTime, BinaryType)] = ???

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
  }

}
