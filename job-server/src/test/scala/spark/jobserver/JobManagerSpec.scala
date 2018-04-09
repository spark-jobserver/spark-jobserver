package spark.jobserver

import java.nio.charset.Charset
import java.nio.file.Files

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
import spark.jobserver.common.akka.AkkaTestUtils

import scala.concurrent.Await

class JobManagerSpec extends FunSpecLike with Matchers with BeforeAndAfter {

  import akka.testkit._
  import com.typesafe.config._

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  implicit var system: ActorSystem = _
  var cluster: Cluster = _
  var clusterAddr: String = _
  val configFile = Files.createTempFile("job-server-config", ".conf")

  before {
    system = ActorSystem("test")
    cluster = Cluster(system)
    clusterAddr = cluster.selfAddress.toString
  }

  after {
    AkkaTestUtils.shutdownAndWait(system)
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
  def waitForTerminationDummy(system: ActorSystem, master: String, deployMode: String) { }
  implicit val timeout: Timeout = 3 seconds

  describe("starting job manager") {
    it ("removes akka.remote.netty.tcp.hostname from config cluster mode") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "akka.remote.netty.tcp.hostname" -> "test"
      ))

      def makeSystem(config: Config): ActorSystem = {
        config.hasPath("akka.remote.netty.tcp.hostname") should be(false)
        system
      }

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSystem, waitForTerminationDummy)
    }

    it ("use temporary sqldao dir in cluster mode") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.sqldao.rootdir" -> "test"
      ))

      def makeSystem(config: Config): ActorSystem = {
        config.getString("spark.jobserver.sqldao.rootdir") shouldNot equal ("test")
        system
      }

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSystem, waitForTerminationDummy)
    }

    it ("keeps hostname and sqldao dir in client mode") {
      val tmpDir = Files.createTempDirectory("job-manager-sqldao").toString
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "client",
        "akka.remote.netty.tcp.hostname" -> "localhost",
        "spark.jobserver.sqldao.rootdir" -> tmpDir
      ))

      def makeSystem(config: Config): ActorSystem = {
        config.getString("akka.remote.netty.tcp.hostname") should equal ("localhost")
        config.getString("spark.jobserver.sqldao.rootdir") should equal (tmpDir)
        system
      }

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSystem, waitForTerminationDummy)
    }

    it ("shouldn't have a configuration for akka remote port") {
      val tmpDir = Files.createTempDirectory("job-manager-sqldao").toString
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "akka.remote.netty.tcp.hostname" -> "localhost",
        "akka.remote.netty.tcp.port" -> "1337",
        "spark.jobserver.sqldao.rootdir" -> tmpDir
      ))

      def makeSystem(config: Config): ActorSystem = {
        config.getInt("akka.remote.netty.tcp.port") should be (0)
        system
      }

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSystem, waitForTerminationDummy)
    }

    it ("starts dao-manager and job manager actor") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster"
      ))

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSupervisorSystem, waitForTerminationDummy)

      Await.result(system.actorSelection("/user/dao-manager-jobmanager").resolveOne, 2 seconds) shouldBe a[ActorRef]
      Await.result(system.actorSelection("/user/test-manager").resolveOne, 2 seconds) shouldBe a[ActorRef]
    }

    it ("joins the akka cluster") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster"
      ))
      val testProbe = TestProbe()
      cluster.subscribe(testProbe.ref, initialStateMode = InitialStateAsEvents, classOf[MemberUp])

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSupervisorSystem, waitForTerminationDummy)

      testProbe.expectMsgClass(classOf[MemberUp])
    }

    it ("calls wait for termination") {
      var called = false
      def waitForTermination(system: ActorSystem, master: String, deployMode: String) {
        called = true
      }
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster"
      ))

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSupervisorSystem, waitForTermination)

      called should be (true)
    }
  }
}
