package spark.jobserver

import akka.actor._
import akka.pattern.AskTimeoutException
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.io.{JobDAO, JobDAOActor}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import scala.concurrent.duration._

import spark.jobserver.common.akka
import spark.jobserver.common.akka.AkkaTestUtils


object AkkaClusterSupervisorSpec {
  val config = ConfigFactory.parseString("""
    akka {
      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
        warn-about-java-serializer-usage = off
      }
    }
    deploy {
      manager-start-cmd = "fake-thing"
    }
    spark {
      master = "local[4]"
      temp-contexts {
        num-cpu-cores = 4           # Number of cores to allocate.  Required.
        memory-per-node = 512m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
      }
      jobserver.job-result-cache-size = 100
      jobserver.context-creation-timeout = 5 s
      jobserver.yarn-context-creation-timeout = 40 s
      jobserver.named-object-creation-timeout = 60 s
      context-per-jvm = true
      contexts {
        olap-demo {
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
        hadoop {
          mapreduce.framework.name = "ayylmao"
        }
      }
    }
    akka.log-dead-letters = 0
    """)

  val system = ActorSystem("test", config)
}


// This test DOES NOT cover actually spawning a seperate JVM,
class AkkaClusterSupervisorSpec extends TestKit(AkkaClusterSupervisorSpec.system) with ImplicitSender
    with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  val theTest = this
  case class SendIdent()
  class StubbedJobManagerActor(name: String, actorName: String, contextConfig: Config) extends Actor {
    import CommonMessages._
    import ContextSupervisor._
    import JobInfoActor._
    import JobManagerActor._

    def receive: PartialFunction[Any, Unit] = {
      case SendIdent => sender ! ActorIdentity(name, Some(self))
      case SparkContextStatus => sender ! SparkContextAlive
      case Initialize(_) =>
        name match {
          case "olap-demo" => sendGood(sender, "olap-demo")
          case "c1" => sendGood(sender, "c1")
          case "c2" => sendGood(sender, "c2")
          case "d1" =>
            Thread.sleep(500)
            sendGood(sender, "d1")
          case "d2" =>
            Thread.sleep(10000)
            sendGood(sender, "d2")
        }
    }
    def sendGood(sender: ActorRef, name: String): Unit = {
      val rActor = system.actorOf(Props(classOf[JobResultActor]))
      sender ! JobManagerActor.Initialized(name, rActor)
    }
  }

  class StubbedAkkaClusterSupervisorActor(daoActor: ActorRef) extends AkkaClusterSupervisorActor(daoActor) {
    override def preStart(): Unit = {
    }

    override def postStop(): Unit = {
    }
    // stub this out for testing, won't be totally
    // accurate as we won't have another process... but close enough :)
    override protected def startContext(
      name: String, actorName: String, contextConfig: Config, isAdHoc: Boolean
    )(successFunc: ActorRef => Unit, failureFunc: Throwable => Unit): Unit = {
      val jobManager = system.actorOf(
        Props(classOf[StubbedJobManagerActor], theTest, name, actorName, contextConfig),
        actorName
      )

      jobManager ! SendIdent
      contextInitInfos(actorName) = (isAdHoc, successFunc, failureFunc)
    }
  }

  override def afterAll() = {
    AkkaTestUtils.shutdownAndWait(AkkaClusterSupervisorSpec.system)
  }

  var supervisor: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _

  val contextConfig = AkkaClusterSupervisorSpec.config.getConfig("spark.context-settings")

  // This is needed to help tests pass on some MBPs when working from home
  System.setProperty("spark.driver.host", "localhost")

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    supervisor = system.actorOf(Props(classOf[StubbedAkkaClusterSupervisorActor], this, daoActor))
  }

  after {
    akka.AkkaTestUtils.shutdownAndWait(supervisor)
  }

  import ContextSupervisor._
  import JobManagerActor._

  describe("akka context management") {
    it("should list empty contexts at startup") {
      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }

    it("can add contexts from jobConfig") {
      supervisor ! AddContextsFromConfig
      Thread sleep 4000
      supervisor ! ListContexts
      expectMsg(40 seconds, Seq("olap-demo"))
    }

    it("should be able to add multiple new contexts") {
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)
      supervisor ! AddContext("c2", contextConfig)
      expectMsg(ContextInitialized)
      supervisor ! ListContexts
      expectMsg(Seq("c1", "c2"))
      supervisor ! GetResultActor("c1")
      val rActor = expectMsgClass(classOf[ActorRef])
      rActor.path.toString should not include "global"
    }

    it("should be able to get context") {
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)
      supervisor ! GetContext("c1")
      expectMsgPF(5 seconds, "I can't find that context :'-(") {
        case (actorName: String, Some(contextActor: ActorRef), Some(resultActor: ActorRef)) =>
          actorName should startWith ("jobManager")
      }
    }

    it("should be able to stop contexts already running") {
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)
      supervisor ! ListContexts
      expectMsg(Seq("c1"))

      supervisor ! StopContext("c1")
      expectMsg(ContextStopped)

      Thread.sleep(2000) // wait for a while since deleting context is an asyc call
      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }

    it("should return NoSuchContext if attempt to stop nonexisting context") {
      supervisor ! StopContext("c1")
      expectMsg(NoSuchContext)
    }

    it("should not allow creation of an already existing context") {
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)

      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextAlreadyExists)
    }

    it("should allow stopping contexts that are not completely started") {
      supervisor ! AddContext("d1", contextConfig)
      Thread.sleep(50)
      supervisor ! StopContext("d1")
      // we expect to see an init and then a stop
      expectMsg(ContextInitialized)
      expectMsg(ContextStopped)
    }

    it("should not allow another context of same name even during it starting") {
      supervisor ! AddContext("d1", contextConfig)
      supervisor ! AddContext("d1", contextConfig)
      expectMsg(ContextAlreadyExists)
      supervisor ! StopContext("d1")
      expectMsg(ContextInitialized)
      expectMsg(ContextStopped)
    }

    it("should remove a context that does not start in time") {
      supervisor ! AddContext("d2", contextConfig)
      supervisor ! ListContexts
      expectMsg(Seq("d2"))
      expectMsgPF(3.seconds, "should have timed out by now...") {
        case ContextInitError(e: Any) =>
          e shouldBe an [AskTimeoutException]
      }
      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }

    it("should still successfully stop a context even if it doesn't start in time") {
      supervisor ! AddContext("d2", contextConfig)
      Thread.sleep(50)
      supervisor ! StopContext("d2")
      expectMsgPF(3.seconds, "should have timed out by now...") {
        case ContextInitError(e: Any) =>
          e shouldBe an [AskTimeoutException]
      }
      expectMsg(ContextStopped)
      supervisor ! ListContexts
      expectMsg(Seq.empty[String])
    }
  }
}
