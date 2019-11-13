package akka.downing

import akka.actor.Address
import akka.cluster.MultiNodeClusterSpec
import akka.downing.KeepOldestTestAutoDown._
import akka.pattern.ask
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Await

trait STMultiNodeSpec extends MultiNodeSpecCallbacks
  with FunSpecLike with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()
}

object SBRMultiJvmSpecConfig extends MultiNodeConfig {
  val first: RoleName = role("first-node")
  val second: RoleName = role("second-node")
  val third: RoleName = role("third-node")
  val fourth: RoleName = role("forth-node")
  val fifth: RoleName = role("fifth-node")

  commonConfig(MultiNodeClusterSpec.baseConfig.withFallback(
    ConfigFactory.parseString("""
      akka {
        cluster.downing-provider-class = akka.downing.KeepOldestTestDowningProvider
      }
      custom-downing {
        stable-after = 3s
        down-removal-margin = 1s
        oldest-auto-downing {
          preferred-oldest-member-role = ""
        }
      }
      """)
  ))

  testTransport(on = true) // to use blackhole (network split)
}

class SBRMultiJvmNode1 extends SBRMultiJvmSpec
class SBRMultiJvmNode2 extends SBRMultiJvmSpec
class SBRMultiJvmNode3 extends SBRMultiJvmSpec
class SBRMultiJvmNode4 extends SBRMultiJvmSpec
class SBRMultiJvmNode5 extends SBRMultiJvmSpec

abstract class SBRMultiJvmSpec extends MultiNodeSpec(SBRMultiJvmSpecConfig)
  with MultiNodeClusterSpec with FunSpecLike with Matchers with ImplicitSender {

  import SBRMultiJvmSpecConfig._
  def initialParticipants: Int = roles.size

  private val downingProviderAddress: String = "/system/cluster/core/daemon/downingProvider"

  private val addressesCache: Map[RoleName, Address] = roles.map(n => n -> node(n).address).toMap
  private val twoNodesCluster: Vector[RoleName] = Vector(first, second)
  private val bigCluster: Vector[RoleName] = Vector(first, second, third, fourth, fifth)

  muteAkkaLogMessages()

  /**
    * Helper function to get the oldest member of the cluster and all the rest of the nodes
    * according to the split brain resolver vision.
    * @param currentCluster - RoleNames of the nodes, which are expected to be in the cluster
    * @return tuple (oldestNode, allOtherNodesInTheCluster), allOtherNodesInTheCluster - still
    *         sorted by age pf the nodes
    */
  private def getOldestAndOtherMembers(currentCluster: Vector[RoleName]): (RoleName, Vector[RoleName]) = {
    val memberByAge = Await.result(
      (system.actorSelection(myAddress + downingProviderAddress) ? GetMembersByAge)
        (timeout), timeout).asInstanceOf[MembersByAge].members
    val oldestMember = currentCluster.filter(n => node(n).address == memberByAge.head.address).head
    val otherMembers = currentCluster.filter(_ != oldestMember)
    (oldestMember, otherMembers)
  }


  describe("Tests for 2 nodes cluster"){

    it ("In cluster of two nodes, the oldest node should shut down itself") {
      runOn(twoNodesCluster: _*) {
        formCluster(twoNodesCluster)
      }
      enterBarrier("Made a cluster of two nodes")

      runOn(twoNodesCluster: _*) {
        system.actorSelection(myAddress + downingProviderAddress) ! InitializeTestActorRef
        expectMsg(timeout, PingTestActor)
      }
      enterBarrier("SBR initialized with testProbe address")

      runOn(first) {
        // split the cluster in two parts
        testConductor.blackhole(twoNodesCluster(0), twoNodesCluster(1), Direction.Both).await
      }
      enterBarrier("Network split made")

      runOn(twoNodesCluster: _*) {
        awaitAssert(
          clusterView.unreachableMembers.map(_.address) should be(
            twoNodesCluster.filter(_ != myself).map(n => addressesCache(n)).toSet), timeout)
      }
      enterBarrier("Nodes see each other as unreachable")

      runOn(twoNodesCluster: _*) {
        val (oldestMember, _) = getOldestAndOtherMembers(twoNodesCluster)
        if (oldestMember == myself) {
          expectMsg(timeout, ShutDownSelfTriggered)
        } else {
          expectMsg(timeout, ShutDownAnotherNode(node(twoNodesCluster.head).address))
        }
      }
      enterBarrier("Oldest node shuts itself down (and is downed by the second oldest)")

      runOn(first) {
        // cleanup: remove network split
        testConductor.passThrough(twoNodesCluster(0), twoNodesCluster(1), Direction.Both).await
      }
      awaitAssert(clusterView.unreachableMembers should ===(Set.empty))
      enterBarrier("Cleaned up. Test finished.")
    }
  }


  describe("Tests for a bigger (>2 nodes) cluster"){
    it ("oldest node should shutdown itself if it is isolated from the rest of the cluster") {
      runOn(bigCluster: _*) {
        formCluster(bigCluster)
      }
      enterBarrier("Made a bigger cluster")

      runOn(bigCluster: _*) {
        val sbr = system.actorSelection(myAddress + downingProviderAddress)
        sbr ! InitializeTestActorRef
        fishForMessage(timeout, "testProbe is waiting for confirmation from SBR"){
          case PingTestActor => true
          case _ => false // as the cluster is reused, some old messages after recovery may be received
        }
      }
      enterBarrier("SBR knows testProbe address")

      runOn(first) {
        val (oldestMember, otherMembers) = getOldestAndOtherMembers(bigCluster)
        // split the cluster in two parts
        for (secondSide <- otherMembers) {
          testConductor.blackhole(oldestMember, secondSide, Direction.Both).await
        }
      }
      enterBarrier("Network split created")

      runOn(bigCluster: _*) {
        val (oldestMember, otherMembers) = getOldestAndOtherMembers(bigCluster)

        if (myself == oldestMember) {
          // oldest node is alone and should shut down itself
          awaitAssert(clusterView.unreachableMembers.map(_.address) should be(
            otherMembers.map(n => node(n).address).toSet), timeout)
          expectMsg(timeout, ShutDownSelfTriggered)
        } else if (otherMembers.head == myself) {
          // secondary oldest node should shutdown the oldest node
          awaitAssert(clusterView.unreachableMembers.map(_.address) should be(
            Set(node(bigCluster.head).address)), timeout)
          expectMsg(timeout, ShutDownAnotherNode(addressesCache(oldestMember)))
        } else {
          // all other nodes should do nothing
          expectNoMsg(timeout)
        }
      }
      enterBarrier("Oldest node shut down itself and secondary oldest downed oldest on the other side.")

      runOn(first) {
        // cleanup: remove network split
        for (secondSide <- bigCluster.tail) {
          testConductor.passThrough(bigCluster.head, secondSide, Direction.Both).await
        }
      }
      awaitAssert(clusterView.unreachableMembers should ===(Set.empty))
      enterBarrier("Cleaned up. Test finished.")
    }

    it ("any node should shutdown itself if it is isolated from the rest of the cluster") {
      runOn(bigCluster: _*) {
        formCluster(bigCluster)
      }
      enterBarrier("Made a bigger cluster")

      runOn(bigCluster: _*) {
        val sbr = system.actorSelection(myAddress + downingProviderAddress)
        sbr ! InitializeTestActorRef
        fishForMessage(timeout, "testProbe is waiting for confirmation from SBR"){
          case PingTestActor => true
          case _ => false // as the cluster is reused, some old messages after recovery may be received
        }
      }
      enterBarrier("SBR knows testProbe address")

      runOn(first) {
        val (_, otherMembers) = getOldestAndOtherMembers(bigCluster)
        val isolatedNode = otherMembers.last
        // split the cluster in two parts
        for (secondSide <- bigCluster.filter(_ != isolatedNode)) {
          testConductor.blackhole(isolatedNode, secondSide, Direction.Both).await
        }
      }
      enterBarrier("Network split created")

      runOn(bigCluster: _*) {
        val (oldestMember, otherMembers) = getOldestAndOtherMembers(bigCluster)
        val isolatedNode = otherMembers.last

        if (myself == isolatedNode) {
          // isolated node should shutdown itself
          awaitAssert(clusterView.unreachableMembers.map(_.address) should be(
            bigCluster.filter(_ != isolatedNode).map(n => node(n).address).toSet), timeout)
          expectMsg(timeout, ShutDownSelfTriggered)
        } else if (myself == oldestMember) {
          // the oldest node should shutdown isolated node
          awaitAssert(clusterView.unreachableMembers.map(_.address) should be(
            Set(addressesCache(isolatedNode))), timeout)
          expectMsg(timeout, ShutDownAnotherNode(addressesCache(isolatedNode)))
        } else {
          // all other nodes should do nothing
          expectNoMsg(timeout)
        }
      }
      enterBarrier("Network split resolved.")

      runOn(first) {
        val (_, otherMembers) = getOldestAndOtherMembers(bigCluster)
        val isolatedNode = otherMembers.last
        // recover from the cluster split
        for (secondSide <- bigCluster.filter(_ != isolatedNode)) {
          testConductor.passThrough(isolatedNode, secondSide, Direction.Both).await
        }
      }
      awaitAssert(clusterView.unreachableMembers should ===(Set.empty))
      enterBarrier("Cleaned up. Test finished.")
    }
  }

  it ("group of nodes should shutdown itself if multiple nodes including the oldest one are unreachable") {
    runOn(bigCluster: _*) {
      formCluster(bigCluster)
    }
    enterBarrier("Made a bigger cluster")

    runOn(bigCluster: _*) {
      val sbr = system.actorSelection(myAddress + downingProviderAddress)
      sbr ! InitializeTestActorRef
      fishForMessage(timeout, "testProbe is waiting for confirmation from SBR"){
        case PingTestActor => true
        case _ => false // as the cluster is reused, some old messages after recovery may be received
      }
    }
    enterBarrier("SBR knows testProbe address")

    runOn(first) {
      val (oldestMember, otherMembers) = getOldestAndOtherMembers(bigCluster)
      val oldestAndSomeNode = Vector(oldestMember, otherMembers.last)
      // split the cluster in two parts
      for (firstSide <- oldestAndSomeNode; secondSide <- bigCluster.filter(!oldestAndSomeNode.contains(_))) {
        testConductor.blackhole(firstSide, secondSide, Direction.Both).await
      }
    }
    enterBarrier("Network split created")

    runOn(bigCluster: _*) {
      val (oldestMember, otherMembers) = getOldestAndOtherMembers(bigCluster)
      val oldestAndSomeNode = Vector(oldestMember, otherMembers.last)

      if (!oldestAndSomeNode.contains(myself)) {
        // node from the split without oldest node should shutdown itself
        awaitAssert(clusterView.unreachableMembers.map(_.address) should be(
          oldestAndSomeNode.map(n => node(n).address).toSet), timeout)
        expectMsg(timeout, ShutDownSelfTriggered)
      } else if (myself == oldestMember) {
        // oldest member should shutdown unreachable nodes
        val isolatedNodesAddresses = bigCluster.filter(!oldestAndSomeNode.contains(_)).map(addressesCache(_))
        awaitAssert(clusterView.unreachableMembers.map(_.address) should be(
          isolatedNodesAddresses.sorted.toSet), timeout)
        for (_ <- 1 to isolatedNodesAddresses.length) {
          expectMsgPF(timeout, "Other nodes were not downed by the oldest node!") {
            case ShutDownAnotherNode(nodeAddress) =>
              isolatedNodesAddresses should contain(nodeAddress)
          }
        }
      } else {
        // all other nodes do nothing
        expectNoMsg()
      }
    }
    enterBarrier("Network split resolved.")

    runOn(first) {
      val (oldestMember, otherMembers) = getOldestAndOtherMembers(bigCluster)
      val oldestAndSomeNode = Vector(oldestMember, otherMembers.last)
      // recover from the cluster split
      for (firstSide <- oldestAndSomeNode; secondSide <- bigCluster.filter(!oldestAndSomeNode.contains(_))) {
        testConductor.passThrough(firstSide, secondSide, Direction.Both).await
      }
    }
    awaitAssert(clusterView.unreachableMembers should ===(Set.empty))
    enterBarrier("Cleaned up. Test finished.")
  }
}