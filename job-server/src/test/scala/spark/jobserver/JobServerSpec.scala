package spark.jobserver

import java.nio.charset.Charset

import scala.util.Try
import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import akka.pattern.ask
import akka.util.Timeout
import akka.testkit.{TestKit, TestProbe, TestActor}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import java.nio.file.Files

import scala.concurrent.duration._
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.common.akka
import spark.jobserver.io.{
  JobDAOActor, JobDAO, ContextInfo, ContextStatus, JobInfo, BinaryInfo, BinaryType, JobStatus}
import spark.jobserver.util.ContextReconnectFailedException


import scala.concurrent.Await
import scala.concurrent.duration.TimeUnit;
import java.util.UUID
import org.joda.time.DateTime
import java.util.concurrent.{TimeUnit, CountDownLatch}

object JobServerSpec {
  val system = ActorSystem("test")
}

class JobServerSpec extends TestKit(JobServerSpec.system) with FunSpecLike with Matchers
  with BeforeAndAfterAll {

  import com.typesafe.config._
  import scala.collection.JavaConverters._

  private val configFile = Files.createTempFile("job-server-config", ".conf")

  override def afterAll() {
    akka.AkkaTestUtils.shutdownAndWait(JobServerSpec.system)
    Files.deleteIfExists(configFile)
  }

  def writeConfigFile(configMap: Map[String, Any]): String = {
    val config = ConfigFactory.parseMap(configMap.asJava).withFallback(ConfigFactory.defaultOverrides())
    Files.write(configFile,
      Seq(config.root.render(ConfigRenderOptions.concise)).asJava,
      Charset.forName("UTF-8"))
    configFile.toAbsolutePath.toString
  }

  def makeSupervisorSystem(config: Config): ActorSystem = system
  implicit val timeout: Timeout = 3 seconds

  describe("Fails on invalid configuration") {
    it("requires context-per-jvm in YARN mode") {
      val configFileName = writeConfigFile(Map(
        "spark.master " -> "yarn",
        "spark.jobserver.context-per-jvm " -> false))

      intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
    }

   it("requires akka.remote.netty.tcp.port in supervise mode") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.context-per-jvm" -> true,
        "spark.driver.supervise" -> true,
        "akka.remote.netty.tcp.port" -> 0))

      val invalidConfException = intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
      invalidConfException.getMessage should
        be("Supervise mode requires akka.remote.netty.tcp.port to be hardcoded")
    }

    it("requires context-per-jvm in Mesos mode") {
      val configFileName = writeConfigFile(Map(
        "spark.master " -> "mesos://test:123",
        "spark.jobserver.context-per-jvm " -> false))

      intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
    }

    it("requires context-per-jvm in cluster mode") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode " -> "cluster",
        "spark.jobserver.context-per-jvm " -> false))

      intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
    }

    it("does not support context-per-jvm and JobFileDAO") {
      val configFileName = writeConfigFile(Map(
        "spark.jobserver.context-per-jvm " -> true,
        "spark.jobserver.jobdao" -> "spark.jobserver.io.JobFileDAO"))

      intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
    }

    it("does not support context-per-jvm and H2 in-memory DB") {
      val configFileName = writeConfigFile(Map(
        "spark.jobserver.context-per-jvm " -> true,
        "spark.jobserver.jobdao" -> "spark.jobserver.io.JobSqlDAO",
        "spark.jobserver.sqldao.jdbc.url" -> "jdbc:h2:mem"))

      intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
    }

    it("does not support cluster mode and H2 in-memory DB") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.context-per-jvm " -> true,
        "spark.jobserver.jobdao" -> "spark.jobserver.io.JobSqlDAO",
        "spark.jobserver.sqldao.jdbc.url" -> "jdbc:h2:mem"))

      intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
    }

    it("does not support cluster mode and H2 file based DB") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.context-per-jvm " -> true,
        "spark.jobserver.jobdao" -> "spark.jobserver.io.JobSqlDAO",
        "spark.jobserver.sqldao.jdbc.url" -> "jdbc:h2:file"))

      intercept[InvalidConfiguration] {
        JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))
      }
    }

    it("starts some actors in local mode") {
      val configFileName = writeConfigFile(Map(
        "spark.master" -> "local[1]",
        "spark.submit.deployMode" -> "client",
        "spark.jobserver.context-per-jvm " -> false,
        "spark.jobserver.sqldao.jdbc.url" -> "jdbc:h2:mem"))

      JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))

      Await.result(system.actorSelection("/user/dao-manager").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(system.actorSelection("/user/data-manager").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(system.actorSelection("/user/binary-manager").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(system.actorSelection("/user/context-supervisor").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(system.actorSelection("/user/job-info").resolveOne, 2 seconds) shouldBe a[ActorRef]
    }

    def createContext(name: String, status: String, genActor: Boolean): ContextInfo = {
      val uuid = UUID.randomUUID().toString()
      var address = "invalidAddress"
      if(genActor) {
        val actor = system.actorOf(Props.empty, name = AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX + uuid)
        address = actor.path.address.toString
      }
      return ContextInfo(uuid, name, "", Some(address), DateTime.now(), None, status, None)
    }

    it("should write error state for contexts and jobs if reconnect failed") {
      val ctxRunning = createContext("ctxRunning", ContextStatus.Running, true)
      val ctxToBeTerminated = createContext("ctxTerminated", ContextStatus.Running, false)

      def genJob(jobId: String, ctx: ContextInfo, status: String) = {
        val dt = DateTime.parse("2013-05-29T00Z")
        JobInfo(jobId, ctx.id, ctx.name, BinaryInfo("demo", BinaryType.Jar, dt), "com.abc.meme",
            status, dt, None, None)
      }
      val jobToBeTerminated = genJob("jid2", ctxToBeTerminated, JobStatus.Running)

      val daoProbe = TestProbe()
      val latch = new CountDownLatch(1)

      var ctxTerminated: ContextInfo = null;
      var jobTerminated: JobInfo = null;

      daoProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case JobDAOActor.GetContextInfos(None, Some(Seq(ContextStatus.Running))) =>
              sender ! JobDAOActor.ContextInfos(Seq(ctxRunning, ctxToBeTerminated))
            case ctxInfo: JobDAOActor.SaveContextInfo => ctxTerminated = ctxInfo.contextInfo
            case JobDAOActor.GetJobInfosByContextId(ctxToBeTerminated.id, Some(states)) =>
              states should equal (JobStatus.getNonFinalStates())
              sender ! JobDAOActor.JobInfos(Seq(jobToBeTerminated))
            case jobInfo: JobDAOActor.SaveJobInfo =>
              jobTerminated = jobInfo.jobInfo
              latch.countDown()
          }
          TestActor.KeepRunning
        }
      })

      JobServer.updateContextStatus(system, daoProbe.ref)
      latch.await()

      ctxTerminated.state should be(ContextStatus.Error)
      ctxTerminated.error.get should be(ContextReconnectFailedException())

      jobTerminated.state should be(JobStatus.Error)
      jobTerminated.error.get.message should be(ContextReconnectFailedException().getMessage)
    }

    it("should return None if no context is available to reconnect") {
      val daoActor = system.actorOf(JobDAOActor.props(new InMemoryDAO))

      val existingClusterAddress = JobServer.updateContextStatus(system, daoActor)
      existingClusterAddress should be(None)
    }

    it("should return actor address (only 1) for actors state running during reconnect") {
      val daoActor = system.actorOf(JobDAOActor.props(new InMemoryDAO))
      val ctxRunning = createContext("ctxRunning", ContextStatus.Running, true)
      val ctxRunning2 = createContext("ctxRunning2", ContextStatus.Running, true)

      daoActor ! JobDAOActor.SaveContextInfo(ctxRunning)
      daoActor ! JobDAOActor.SaveContextInfo(ctxRunning2)

      val existingClusterAddress = JobServer.updateContextStatus(system, daoActor)
      existingClusterAddress.get should be(ctxRunning.actorAddress.get)
    }
  }
}
