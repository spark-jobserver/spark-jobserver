package spark.jobserver

import java.nio.charset.Charset

import scala.util.Try
import akka.actor.{Actor, ActorRef, ActorSystem, Address, Props}
import akka.cluster.{Cluster, ClusterEvent, MemberStatus}
import akka.pattern.ask
import akka.util.Timeout
import akka.testkit.{TestActor, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import java.nio.file.{Files, Path}

import scala.concurrent.duration._
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.common.akka
import spark.jobserver.io.{BinaryInfo, BinaryType, ContextInfo, ContextStatus, JobDAO, JobDAOActor, JobInfo, JobStatus}
import spark.jobserver.util.{ContextReconnectFailedException, DAOCleanup}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import java.util.UUID

import org.joda.time.DateTime
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.config.Config

object JobServerSpec {
  val system = ActorSystem("test")
}

class JobServerSpec extends TestKit(JobServerSpec.system) with FunSpecLike with Matchers
    with BeforeAndAfter with BeforeAndAfterAll {

  import com.typesafe.config._
  import scala.collection.JavaConverters._

  private var configFile: Path = null
  private var actorSystem: ActorSystem = null

  before {
    configFile = Files.createTempFile("job-server-config", ".conf")
    actorSystem = ActorSystem("JobServerSpec")
  }

  after {
    akka.AkkaTestUtils.shutdownAndWait(actorSystem)
    Files.deleteIfExists(configFile)
  }

  override def afterAll() {
    akka.AkkaTestUtils.shutdownAndWait(JobServerSpec.system)
  }

  def writeConfigFile(configMap: Map[String, Any]): String = {
    val config = ConfigFactory.parseMap(configMap.asJava).withFallback(ConfigFactory.defaultOverrides())
    Files.write(configFile,
      Seq(config.root.render(ConfigRenderOptions.concise)).asJava,
      Charset.forName("UTF-8"))
    configFile.toAbsolutePath.toString
  }

  def resolveActorRef(actorPath: String, maxIterations: Int = 30): Option[ActorRef] = {
    for (_ <- 1 to maxIterations) {
      Try(Some(Await.result(
        actorSystem.actorSelection(actorPath).resolveOne, 2.seconds))).getOrElse(None) match {
        case None => Thread.sleep(500)
        case ref => return ref
      }
    }
    return None
  }

  def makeSupervisorSystem(config: Config): ActorSystem = {
    val configWithRole = config.withValue("akka.cluster.roles",
      ConfigValueFactory.fromIterable(List("supervisor").asJava))
    actorSystem = ActorSystem("JobServerSpec", configWithRole)
    actorSystem
  }

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

      Await.result(actorSystem.actorSelection("/user/dao-manager").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(actorSystem.actorSelection("/user/data-manager").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(actorSystem.actorSelection("/user/binary-manager").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(actorSystem.actorSelection("/user/context-supervisor").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(actorSystem.actorSelection("/user/job-info").resolveOne, 2 seconds) shouldBe a[ActorRef]
    }

    it("should startup correctly in other modes (client/cluster)") {
      val configFileName = writeConfigFile(Map(
        "spark.master" -> "local[1]",
        "spark.submit.deployMode" -> "client",
        "spark.jobserver.context-per-jvm " -> true,
        "spark.jobserver.sqldao.jdbc.url" -> "jdbc:h2:file"))

      JobServer.start(Seq(configFileName).toArray, makeSupervisorSystem(_))

      resolveActorRef("/user/dao-manager") shouldNot be(None)
      resolveActorRef("/user/data-manager") shouldNot be(None)
      resolveActorRef("/user/binary-manager") shouldNot be(None)
      resolveActorRef("/user/singleton") shouldNot be (None)
      resolveActorRef("/user/singleton/context-supervisor") shouldNot be (None)
      resolveActorRef("/user/context-supervisor-proxy") shouldNot be (None)
      resolveActorRef("/user/job-info") shouldNot be(None)
    }

    def createContext(name: String, status: String, genActor: Boolean): (ContextInfo, Option[ActorRef]) = {
      val uuid = UUID.randomUUID().toString()
      val ContextInfoPF = ContextInfo(uuid, name, "", _: Option[String], DateTime.now(), None, status, None)

      genActor match {
        case true =>
          val actor = actorSystem.actorOf(Props.empty, name = AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX + uuid)
          (ContextInfoPF(Some(actor.path.address.toString)), Some(actor))
        case false =>
          (ContextInfoPF(Some("invalidAddress")), None)
      }
    }

    def genJob(jobId: String, ctx: ContextInfo, status: String): JobInfo = {
      val dt = DateTime.parse("2013-05-29T00Z")
      JobInfo(jobId, ctx.id, ctx.name, BinaryInfo("demo", BinaryType.Jar, dt), "com.abc.meme",
          status, dt, None, None)
    }

    it("should write error state for contexts and jobs if reconnect failed") {
      val (ctxRunning, ctxRunningActorRef) = createContext("ctxRunning", ContextStatus.Running, true)
      val (ctxStopping, ctxStoppingActorRef) = createContext("ctxStopping", ContextStatus.Stopping, true)
      val (ctxToBeTerminated, _) = createContext("ctxTerminated", ContextStatus.Running, false)
      val (ctxAlreadyStopped, ctxStoppingInvalidActorRef) =
        createContext("ctxStoppingInvalid", ContextStatus.Stopping, false)

      def genJob(jobId: String, ctx: ContextInfo, status: String) = {
        val dt = DateTime.parse("2013-05-29T00Z")
        JobInfo(jobId, ctx.id, ctx.name, BinaryInfo("demo", BinaryType.Jar, dt), "com.abc.meme",
            status, dt, None, None)
      }

      val jobToBeTerminated = genJob("jid2", ctxToBeTerminated, JobStatus.Running)
      val jobOfAlreadyStoppedCtx = genJob("jid3", ctxAlreadyStopped, JobStatus.Running)

      val daoProbe = TestProbe()
      val latch = new CountDownLatch(2)

      var ctxTerminated: ContextInfo = null
      var ctxAlreadyStoppedUpdated: ContextInfo = null
      var jobTerminated: JobInfo = null
      var jobAlreadyStoppedUpdated: JobInfo = null

      daoProbe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case JobDAOActor.GetContextInfos(None,
                Some(Seq(ContextStatus.Running, ContextStatus.Stopping))) =>
              sender ! JobDAOActor.ContextInfos(
                Seq(ctxRunning, ctxToBeTerminated, ctxStopping, ctxAlreadyStopped))
            case ctxInfo: JobDAOActor.SaveContextInfo =>
              ctxInfo.contextInfo.id match {
                case ctxToBeTerminated.id => ctxTerminated = ctxInfo.contextInfo
                case ctxAlreadyStopped.id => ctxAlreadyStoppedUpdated = ctxInfo.contextInfo
              }
            case JobDAOActor.GetJobInfosByContextId(id, Some(states)) =>
              id match {
                case ctxToBeTerminated.id =>
                  states should equal (JobStatus.getNonFinalStates())
                  sender ! JobDAOActor.JobInfos(Seq(jobToBeTerminated))
                case ctxAlreadyStopped.id =>
                  states should equal (JobStatus.getNonFinalStates())
                  sender ! JobDAOActor.JobInfos(Seq(jobOfAlreadyStoppedCtx))
              }

            case jobInfo: JobDAOActor.SaveJobInfo =>
              jobInfo.jobInfo.jobId match {
                case jobToBeTerminated.jobId => jobTerminated = jobInfo.jobInfo
                case jobOfAlreadyStoppedCtx.jobId => jobAlreadyStoppedUpdated = jobInfo.jobInfo
              }
              latch.countDown()
          }
          TestActor.KeepRunning
        }
      })

      val existingManagerActorRefs = JobServer.getExistingManagerActorRefs(actorSystem, daoProbe.ref)
      latch.await()

      existingManagerActorRefs.length should be(2)
      existingManagerActorRefs should contain allElementsOf
        List(ctxRunningActorRef.get, ctxStoppingActorRef.get)

      ctxTerminated.state should be(ContextStatus.Error)
      ctxTerminated.error.get should be(ContextReconnectFailedException())

      jobTerminated.state should be(JobStatus.Error)
      jobTerminated.error.get.message should be(ContextReconnectFailedException().getMessage)

      ctxAlreadyStoppedUpdated.state should be(ContextStatus.Error)
      ctxAlreadyStoppedUpdated.error.get should be(ContextReconnectFailedException())

      jobAlreadyStoppedUpdated.state should be(JobStatus.Error)
      jobAlreadyStoppedUpdated.error.get.message should be(ContextReconnectFailedException().getMessage)
    }

    it("should return empty list if no context is available to reconnect") {
      val daoActor = actorSystem.actorOf(JobDAOActor.props(new InMemoryDAO))

      val existingManagerActorRefs = JobServer.getExistingManagerActorRefs(actorSystem, daoActor)
      existingManagerActorRefs should be(List())
    }

    it("should update the status of context and jobs if reconnection failed") {
      val daoActorProbe = TestProbe()

      // Run setReconnectionFailedForContextAndJobs in separate thread
      // to be able to probe the ongoing communication properly (especially due to blocking awaits)
      val (ctxRunning, _) = createContext("ctxRunning", ContextStatus.Running, true)
      Future{
        JobServer.setReconnectionFailedForContextAndJobs(ctxRunning, daoActorProbe.ref, timeout)
      }

      // Check if context is updated correctly
      val saveContextMsg = daoActorProbe.expectMsgType[JobDAOActor.SaveContextInfo]
      saveContextMsg.contextInfo.id should be(ctxRunning.id)
      saveContextMsg.contextInfo.state should be(ContextStatus.Error)
      saveContextMsg.contextInfo.error.get.getMessage should be(ContextReconnectFailedException().getMessage)

      // Check if job is updated correctly
      val runningJobInfo = genJob("jid1", ctxRunning, JobStatus.Running)
      val runningJobInfo2 = genJob("jid2", ctxRunning, JobStatus.Running)
      daoActorProbe.expectMsgType[JobDAOActor.GetJobInfosByContextId]
      daoActorProbe.reply(JobDAOActor.JobInfos(Seq(runningJobInfo, runningJobInfo2)))

      val saveJobMsg = daoActorProbe.expectMsgType[JobDAOActor.SaveJobInfo]
      saveJobMsg.jobInfo.state should be(JobStatus.Error)
      saveJobMsg.jobInfo.error.get.message should be(ContextReconnectFailedException().getMessage)

      val saveJobMsg2 = daoActorProbe.expectMsgType[JobDAOActor.SaveJobInfo]
      saveJobMsg2.jobInfo.state should be(JobStatus.Error)
      saveJobMsg2.jobInfo.error.get.message should be(ContextReconnectFailedException().getMessage)
    }

    it("should resolve valid actor reference correctly") {
      val (ctxRunning, ctxRunningActorRef) = createContext("ctxRunning", ContextStatus.Running, true)
      val (ctxInvalidAddress, ctxInvalidAddressRef) = createContext("ctxInvalidAddress", ContextStatus.Running, false)
      val actorRef = JobServer.getManagerActorRef(ctxRunning, actorSystem)

      actorRef should not be(None)
      actorRef.get.actorRef.path.address.toString should be(ctxRunningActorRef.get.path.address.toString)

      JobServer.getManagerActorRef(ctxInvalidAddress, actorSystem) should be(None)
    }
  }

  describe("Dao cleanup tests") {
    it("should correctly find if the cleanup is enabled or not") {
      JobServer.isCleanupEnabled(ConfigFactory.parseString("")) should be(false)
      JobServer.isCleanupEnabled(
        ConfigFactory.parseString("spark.jobserver.startup_dao_cleanup_class=\"\"")) should be(false)
      JobServer.isCleanupEnabled(ConfigFactory.parseString(
        "spark.jobserver.startup_dao_cleanup_class=spark.jobserver.util.ZKCleanup")) should be(true)
    }

    it("should create an instance of DAOCleanup correctly") {
      val config = ConfigFactory.parseString(
        s"spark.jobserver.startup_dao_cleanup_class=${classOf[dummyCleanup].getName}")
      JobServer.doStartupCleanup(config) should be(true)
    }

    it("should fail if wrong class is passed in") {
      val config = ConfigFactory.parseString(
        s"spark.jobserver.startup_dao_cleanup_class=wrongClass")
      intercept[ClassNotFoundException] {
        JobServer.doStartupCleanup(config)
      }
    }
  }

  describe("Join Akka Cluster tests") {
    it("should join itself if no slave is available and no HA setup is configured") {
      val cluster = Cluster(actorSystem)
      cluster.subscribe(testActor, ClusterEvent.InitialStateAsSnapshot, classOf[ClusterEvent.MemberEvent])
      expectMsgClass(classOf[ClusterEvent.CurrentClusterState])

      JobServer.joinAkkaCluster(cluster, List.empty, List.empty)

      expectMsgClass(classOf[ClusterEvent.MemberUp])

      within(5.seconds) {
        awaitAssert {
          cluster.state.members.size should ===(1)
          cluster.state.members.head.status should ===(MemberStatus.Up)
          cluster.state.members.head.address === cluster.selfAddress
        }
      }
    }
  }
}

class dummyCleanup(config: Config) extends DAOCleanup {
  override def cleanup(): Boolean = {
    true
  }
}
