package spark.jobserver

import akka.actor._
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.testkit._

import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.{JobDAO, JobDAOActor}
import ContextSupervisor._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.{Matchers, FunSpec, BeforeAndAfter, BeforeAndAfterAll, FunSpecLike}


object AkkaClusterSupervisorActorSpec {
  // All the Actors System should have the same name otherwise they cannot form a cluster
  val ACTOR_SYSTEM_NAME = "test"

  val config = ConfigFactory.parseString("""
    akka {
      # Disable all akka output to console
      log-dead-letters = 0
      loglevel = "OFF" # Other options INFO, OFF, DEBUG, WARNING
      stdout-loglevel = "OFF"
      log-dead-letters-during-shutdown = off
      cluster.log-info = off
      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
        warn-about-java-serializer-usage = off
      }
      remote.netty.tcp.hostname = "127.0.0.1"
    }
    spark {
      master = "local[4]"
      temp-contexts {
        num-cpu-cores = 4           # Number of cores to allocate.  Required.
        memory-per-node = 512m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
      }
      jobserver.job-result-cache-size = 100
      jobserver.context-creation-timeout = 5 s
      jobserver.dao-timeout = 3 s
      context-per-jvm = true
      contexts {
        config-context {
          num-cpu-cores = 4
          memory-per-node = 512m
        }
      }
      context-settings {
        num-cpu-cores = 2
        memory-per-node = 512m
        context-init-timeout = 2 s
        context-factory = spark.jobserver.context.DefaultSparkContextFactory
        passthrough {
          spark.driver.allowMultipleContexts = true
          spark.ui.enabled = false
        }
      }
    }
    """)

  val system = ActorSystem(ACTOR_SYSTEM_NAME, config)
}

class StubbedAkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef, managerProbe: TestProbe)
        extends AkkaClusterSupervisorActor(daoActor, dataManagerActor) {

  def createSlaveClusterWithJobManager(contextName: String, contextConfig: Config): (Cluster, ActorRef) = {
    val managerConfig = ConfigFactory.parseString("akka.cluster.roles=[manager],akka.remote.netty.tcp.port=0").withFallback(config)
    val managerSystem = ActorSystem(AkkaClusterSupervisorActorSpec.ACTOR_SYSTEM_NAME, managerConfig)

    val stubbedJobManagerRef = managerSystem.actorOf(Props(classOf[StubbedJobManagerActor], contextConfig), contextName)
    val cluster = Cluster(managerSystem)
    managerProbe.watch(stubbedJobManagerRef)
    (cluster, stubbedJobManagerRef)
  }

    override protected def launchDriver(name: String, contextConfig: Config, contextActorName: String): (Boolean, String) = {
      // Create probe and cluster and join back the master
      Try(contextConfig.getBoolean("driver.fail")).getOrElse(false) match {
        case true => (false, "")
        case false =>
          val managerActorAndCluster = createSlaveClusterWithJobManager(contextActorName, contextConfig)
          managerActorAndCluster._1.join(selfAddress)
          (true, "")
      }
    }
  }

class StubbedJobManagerActor(contextConfig: Config) extends Actor {
  def receive = {
    case JobManagerActor.Initialize(contextConfig,_,_) =>
      val resultActor = context.system.actorOf(Props(classOf[JobResultActor]))
      sender() ! JobManagerActor.Initialized(contextConfig.getString("context.name"), resultActor)
    case JobManagerActor.GetContexData =>
      val appId = Try(contextConfig.getString("manager.context.appId")).getOrElse("")
      val webUiUrl = Try(contextConfig.getString("manager.context.webUiUrl")).getOrElse("")
      (appId, webUiUrl) match {
        case ("", "") => sender() ! JobManagerActor.SparkContextDead
        case (_, "") => sender() ! JobManagerActor.ContexData(appId, None)
        case (_, _) => sender() ! JobManagerActor.ContexData(appId, Some(webUiUrl))
      }
  }
}

class AkkaClusterSupervisorActorSpec extends TestKit(AkkaClusterSupervisorActorSpec.system) with ImplicitSender
      with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  val contextInitTimeout = 10.seconds.dilated
  var supervisor: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _
  var managerProbe = TestProbe()
  val contextConfig = AkkaClusterSupervisorActorSpec.config.getConfig("spark.context-settings")

  // This is needed to help tests pass on some MBPs when working from home
  System.setProperty("spark.driver.host", "localhost")


  override def beforeAll() {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    supervisor = system.actorOf(Props(classOf[StubbedAkkaClusterSupervisorActor], daoActor, TestProbe().ref, managerProbe), "supervisor")
  }

  override def afterAll() = {
     AkkaTestUtils.shutdownAndWait(AkkaClusterSupervisorActorSpec.system)
  }

  after {
    // Cleanup all the context to have a fresh start for next testcase
    def stopContext(contextName: Any) {
      supervisor ! StopContext(contextName.toString())
      expectMsg(3.seconds.dilated, ContextStopped)
      managerProbe.expectMsgClass(classOf[Terminated])
    }

    supervisor ! ListContexts
    expectMsgPF(3.seconds.dilated) {
      case contexts: ArrayBuffer[_] => contexts.foreach(stopContext(_))
      case _ =>
    }
  }

  describe("Context create tests") {
    it("should be able to start a context") {
      supervisor ! AddContext("test-context", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)
    }

    it("should return valid managerActorRef and resultActorRef if context exists") {
      supervisor ! AddContext("test-context1", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! GetContext("test-context1")
      val isValid = expectMsgPF(2.seconds.dilated) {
        case (jobManagerActor: ActorRef, resultActor: ActorRef) => true
        case _ => false
      }

      isValid should be (true)
    }

    it("should not create context in case of error") {
      val wrongConfig = ConfigFactory.parseString("driver.fail=true").withFallback(contextConfig)
      supervisor ! AddContext("test-context2", wrongConfig)
      expectMsgClass(classOf[ContextInitError])

      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }

    it("should not start two contexts with the same name") {
      supervisor ! AddContext("test-context3", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! AddContext("test-context3", contextConfig)
      expectMsg(contextInitTimeout, ContextAlreadyExists)
    }

    it("should be able to add contexts from config") {
      supervisor ! AddContextsFromConfig
      Thread.sleep(contextInitTimeout.toMillis) // AddContextsFromConfig does not return any message

      supervisor ! ListContexts
      expectMsg(Seq("config-context"))
    }

    it("should be able to start adhoc context and list it") {
      import spark.jobserver.util.SparkJobUtils
      supervisor ! StartAdHocContext("test-adhoc-classpath", ConfigFactory.parseString(""))

      val isValid = expectMsgPF(contextInitTimeout, "manager and result actors") {
        case (manager: ActorRef, resultActor: ActorRef) =>
          manager.path.name.startsWith("jobManager-")
      }

      isValid should be (true)

      supervisor ! ListContexts
      val hasContext = expectMsgPF(3.seconds.dilated) {
        case contexts: ArrayBuffer[_] =>
          contexts.head.toString().endsWith("test-adhoc-classpath")
        case _ => false
      }
      hasContext should be (true)
    }

    it("should be able to stop a running context") {
      supervisor ! AddContext("test-context4", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! StopContext("test-context4")
      expectMsg(ContextStopped)
    }

    it("context stop should be able to handle case when no context is present") {
      supervisor ! StopContext("test-context5")
      expectMsg(NoSuchContext)
    }

    it("should be able to start multiple contexts") {
      supervisor ! AddContext("test-context6", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! AddContext("test-context7", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! ListContexts
      expectMsgAnyOf(ArrayBuffer("test-context6", "test-context7"), ArrayBuffer("test-context7", "test-context6"))
    }
  }

  describe("Other context operations tests") {
    it("should list empty context at startup") {
       supervisor ! ListContexts
       expectMsg(Seq.empty[String])
    }

    it("should return NoSuchContext if context is not available while processing GetContext") {
       supervisor ! GetContext("dummy-name")
       expectMsg(NoSuchContext)
    }

    it("should be able to list all the started contexts") {
      supervisor ! AddContext("test-context8", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! ListContexts
      expectMsg(Seq("test-context8"))
    }

    it("should return valid result actor") {
      supervisor ! AddContext("test-context9", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! GetResultActor("test-context9")
      expectMsgClass(classOf[ActorRef])
    }

    it("should return NoSuchContext if context is not available for GetSparkContextInfo") {
      supervisor ! GetSparkContexData("dummy-name")
      expectMsg(NoSuchContext)
    }

    it("should return valid appId and webUiUrl if context is running") {
      val configWithContextInfo = ConfigFactory.parseString("manager.context.webUiUrl=dummy-url,manager.context.appId=appId-dummy")
                .withFallback(contextConfig)
      supervisor ! AddContext("test-context10", configWithContextInfo)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! GetSparkContexData("test-context10")
      expectMsg(SparkContexData("test-context10", "appId-dummy", Some("dummy-url")))
    }

    it("should return NoSuchContext if the context is dead") {
      supervisor ! AddContext("test-context11", contextConfig)
      expectMsg(contextInitTimeout, ContextInitialized)

      supervisor ! GetSparkContexData("test-context11")
      // JobManagerActor Stub by default return NoSuchContext
      expectMsg(NoSuchContext)
    }
  }
}
