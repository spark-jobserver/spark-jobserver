package spark.jobserver

import java.nio.charset.Charset

import scala.util.Try
import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import akka.pattern.ask
import akka.util.Timeout
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import java.nio.file.Files

import scala.concurrent.duration._
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.common.akka
import spark.jobserver.io.{JobDAOActor, JobDAO, ContextInfo, ContextStatus}

import scala.concurrent.Await
import scala.concurrent.duration.TimeUnit;
import java.util.UUID
import org.joda.time.DateTime
import java.util.concurrent.TimeUnit

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

    it("reconnect to actors after restart") {
      val daoActor = system.actorOf(JobDAOActor.props(new InMemoryDAO))

      // Add two contexts in Running mode into db, but only initialize one actor
      val ctxRunning = createContext("ctxRunning", ContextStatus.Running, true)
      val ctxTerminated = createContext("ctxTerminated", ContextStatus.Running, false)
      daoActor ! JobDAOActor.SaveContextInfo(ctxRunning)
      daoActor ! JobDAOActor.SaveContextInfo(ctxTerminated)

      JobServer.updateContextStatus(daoActor, system)

      val timeout = Timeout.apply(Duration.create(3, TimeUnit.SECONDS))
      val resp = Await.result((daoActor ? JobDAOActor.GetContextInfos(None, None))(timeout).
          mapTo[JobDAOActor.ContextInfos], timeout.duration)
      // Expect that only the context with the initialized actor is in the running state
      resp.contextInfos.size should equal (2)
      resp.contextInfos.foreach(ci => {
        if (ctxRunning.name.equals(ci.name)) {
          ci.state should equal (ContextStatus.Running)
        } else {
          ci.state should equal (ContextStatus.Error)
          ci.error.get.getMessage should be ("Reconnect failed after Jobserver restart")
        }
      })
    }
  }
}
