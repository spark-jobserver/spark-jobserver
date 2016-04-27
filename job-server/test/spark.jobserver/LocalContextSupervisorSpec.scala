package spark.jobserver

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import spark.jobserver.io.{JobDAO, JobDAOActor}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._


object LocalContextSupervisorSpec {
  val config = ConfigFactory.parseString("""
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
      contexts {
        olap-demo {
          num-cpu-cores = 4
          memory-per-node = 512m
        }
      }

      context-settings {
        num-cpu-cores = 2
        memory-per-node = 512m
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

class LocalContextSupervisorSpec extends TestKit(LocalContextSupervisorSpec.system) with ImplicitSender
    with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  override def afterAll() {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(LocalContextSupervisorSpec.system)
  }

  var supervisor: ActorRef = _
  var dao: JobDAO = _
  var daoActor: ActorRef = _

  val contextConfig = LocalContextSupervisorSpec.config.getConfig("spark.context-settings")

  // This is needed to help tests pass on some MBPs when working from home
  System.setProperty("spark.driver.host", "localhost")

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    supervisor = system.actorOf(Props(classOf[LocalContextSupervisorActor], daoActor))
  }

  after {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(supervisor)
  }

  import ContextSupervisor._
  import JobManagerActor._

  describe("context management") {
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
      // serializing the creation at least until SPARK-2243 gets
      // solved.
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

    it("should be able to get context configs") {
      supervisor ! AddContext("c1", contextConfig)
      expectMsg(ContextInitialized)
      supervisor ! GetContext("c1")
      expectMsgPF(5 seconds, "I can't find that context :'-(") {
        case (contextActor: ActorRef, resultActor: ActorRef) =>
          contextActor ! GetContextConfig
          val cc = expectMsgClass(classOf[ContextConfig])
          cc.contextName shouldBe "c1"
          cc.contextConfig.get("spark.ui.enabled") shouldBe "false"
          cc.hadoopConfig.get("mapreduce.framework.name") shouldBe "ayylmao"
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
  }
}
