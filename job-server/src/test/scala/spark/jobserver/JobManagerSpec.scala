package spark.jobserver

import java.nio.charset.Charset
import java.nio.file.Files
import java.security.Permission

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp}
import akka.util.Timeout
import com.google.common.net.InetAddresses
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Await
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.util.HDFSCluster

sealed case class JVMExitException(status: Int) extends SecurityException("sys.exit() is not allowed") {
}

sealed class NoExitSecurityManager extends SecurityManager {
  override def checkPermission(perm: Permission): Unit = {}

  override def checkPermission(perm: Permission, context: Object): Unit = {}

  override def checkExit(status: Int): Unit = {
    super.checkExit(status)
    throw JVMExitException(status)
  }
}

class JobManagerSpec extends FunSpecLike with Matchers with BeforeAndAfter
    with BeforeAndAfterAll with HDFSCluster {

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

  override def beforeAll(): Unit = System.setSecurityManager(new NoExitSecurityManager)

  override def afterAll(): Unit = System.setSecurityManager(null)

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
    it ("should set hostname to empty string when Akka network strategy is used") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.network-address-resolver" -> "AKKA",
        "akka.remote.netty.tcp.hostname" -> "test"
      ))

      def makeSystem(config: Config): ActorSystem = {
        config.getString("akka.remote.netty.tcp.hostname") should be("")
        system
      }

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSystem, waitForTerminationDummy)
    }

    it ("should not change hostname when Manual network strategy is used") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.network-address-resolver" -> "manual",
        "akka.remote.netty.tcp.hostname" -> "test"
      ))

      def makeSystem(config: Config): ActorSystem = {
        config.getString("akka.remote.netty.tcp.hostname") should be("test")
        system
      }

      JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
        makeSystem, waitForTerminationDummy)
    }

    it ("should set current hostname when Auto network strategy is used") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.network-address-resolver" -> "auto",
        "akka.remote.netty.tcp.hostname" -> "test"
      ))

      def makeSystem(config: Config): ActorSystem = {
        val updatedHostname = config.getString("akka.remote.netty.tcp.hostname")
        updatedHostname should not be("test")
        InetAddresses.isInetAddress(updatedHostname) should be(true)
        system
      }

      try {
        JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
          makeSystem, waitForTerminationDummy)
      } catch {
        case _: JVMExitException =>
          println("WARN: Cannot run this test as no valid externally accessible interface is available")
      }
    }

    it ("should exit if unsupported network strategy is used") {
      val configFileName = writeConfigFile(Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.jobserver.network-address-resolver" -> "unsupported",
        "akka.remote.netty.tcp.hostname" -> "test"
      ))

      def makeSystem(config: Config): ActorSystem = {
        fail("Cannot reach this point as JVM should already be exited")
        system
      }

      intercept[JVMExitException] {
        JobManager.start(Seq(clusterAddr, "test-manager", configFileName).toArray,
          makeSystem, waitForTerminationDummy)
      }
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

    it("should exit jvm if config file cannot be loaded") {
      failOnWrongPath("")
      failOnWrongPath("file:wrong_path")
      failOnWrongPath("file:/wrong_path")
      failOnWrongPath("hdfs:///wrong_path")
      failOnWrongPath("hdfs://localhost:8020/wrong_path")

      def failOnWrongPath(configPath: String): Unit = {
        def makeSystem(config: Config): ActorSystem = {
          fail("Cannot reach this point as JVM should already be exited")
          system
        }

        intercept[JVMExitException] {
          JobManager.start(Seq(clusterAddr, "test-manager", configPath).toArray,
            makeSystem, waitForTerminationDummy)
        }
      }
    }

    it("should be able to load config file from hadoop supported file systems") {
      val configFilePath = writeConfigFile(Map(
        "spark.jobserver.hdfs.test" -> "Wohoo!"
      ))

      super.startHDFS()
      val configHDFSDir = s"${super.getNameNodeURI()}/spark-jobserver"
      super.writeFileToHDFS(configFilePath, configHDFSDir)
      val configHDFSPath = s"${configHDFSDir}/${configFile.getFileName.toString}"

      JobManager.start(Seq(clusterAddr, "test-manager", configHDFSPath).toArray,
        makeSystem, waitForTerminationDummy)

      def makeSystem(config: Config): ActorSystem = {
        config.getString("spark.jobserver.hdfs.test") should be("Wohoo!")
        system
      }

      super.shutdownHDFS()
    }
  }
}
