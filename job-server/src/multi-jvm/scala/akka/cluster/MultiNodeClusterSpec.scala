package akka.cluster

import akka.actor.Address
import akka.downing.SBRMultiJvmSpecConfig.testTransport
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.EventFilter
import akka.testkit.TestEvent.Mute
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._

trait STMultiNodeSpec extends MultiNodeSpecCallbacks
  with FunSpecLike with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()
  override def afterAll(): Unit = multiNodeSpecAfterAll()
}

object MultiNodeClusterSpec {
  val baseConfig: Config = ConfigFactory.parseString(s"""
    akka {
      # Disable all akka output to console
      loglevel = "OFF" # Other options INFO, OFF, DEBUG, WARNING
      stdout-loglevel = "OFF"
      log-dead-letters = off
      log-dead-letters-during-shutdown = off

      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
        warn-about-java-serializer-usage = off
      }
      remote.netty.tcp.hostname = "127.0.0.1"
      cluster {
        log-info                            = off
        jmx.enabled                         = off
        gossip-interval                     = 100 ms
        leader-actions-interval             = 100 ms
        unreachable-nodes-reaper-interval   = 300 ms
        periodic-tasks-initial-delay        = 150 ms
        publish-stats-interval              = 0 s # always, when it happens
        failure-detector.heartbeat-interval = 100 ms
        run-coordinated-shutdown-when-down = off
        failure-detector.threshold = 2
      }
    }
    spark.driver.supervise = false
    """)

  testTransport(on = true) // to use blackhole
}

trait MultiNodeClusterSpec extends STMultiNodeSpec {
  self: MultiNodeSpec =>
    val timeout: FiniteDuration = 5.seconds

    def cluster: Cluster = Cluster(system)
    def clusterView: ClusterReadView = cluster.readView

    def muteAkkaLogMessages(): Unit = {
      system.eventStream.publish(Mute(EventFilter.error(pattern = ".*Marking.* as UNREACHABLE.*")))
      system.eventStream.publish(Mute(EventFilter.info(pattern = ".*Marking.* as REACHABLE.*")))
      system.eventStream.publish(Mute(EventFilter.warning(pattern = ".*received dead letter from .*")))
      system.eventStream.publish(Mute(EventFilter.warning(pattern = ".*unhandled message from.*")))
    }

  /**
    * Expected to be run on the first (controller) node. Will first create a cluster
    * from the first node and then let all the rest of the node join it.
    * @param nodes list of RoleNames which should form a cluster
    */
    def formCluster(nodes: Seq[RoleName]): Unit = {
      val nodesAddresses: TreeSet[Address] =
        collection.immutable.TreeSet[Address]() ++ nodes.map(node(_).address)
      runOn(nodes.head) {
        cluster.join(node(myself).address)
        awaitAssert(clusterView.members.map(_.address) should contain(node(myself).address))
      }

      runOn(nodes.tail: _*) {
        cluster.join(node(nodes.head).address)
      }

      awaitAssert(clusterView.members.collect{
        case m if m.status == MemberStatus.up => m.address
      } should be (nodesAddresses), timeout)
    }
}
