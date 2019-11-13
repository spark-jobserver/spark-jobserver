/**
  * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
  * 2016- Modified by Yusuke Yasuda
  *
  * This code is a modified version of parts of "Akka-cluster-custom-downing" project:
  * https://github.com/TanUkkii007/akka-cluster-custom-downing/blob/master/src/test/scala/
  * tanukki/akka/cluster/autodown/OldestAutoDownRolesSpec.scala
  * and
  * https://github.com/akka/akka/blob/master/akka-cluster/src/test/scala/akka/cluster/AutoDownSpec.scala
  */

package akka.downing

import akka.actor.{ActorRef, Address, Scheduler, _}
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus._
import akka.cluster.{Member, TestMember}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.collection.immutable
import scala.concurrent.duration.{FiniteDuration, _}


case object ShutDownCausedBySplitBrainResolver
case class DownCalled(address: Address)
case class DownCalledBySecondaryOldest(address: Address)
case class DownNode(nodeAddress: Address, downedBy: Address)

class OldestAutoDownTestActor(address: Address,
                              autoDownUnreachableAfter: FiniteDuration,
                              oldestMemberRole: Option[String],
                              probe: ActorRef)
  extends KeepOldestAutoDown(oldestMemberRole, true, autoDownUnreachableAfter) {

  override def selfAddress: Address = address
  override def scheduler: Scheduler = context.system.scheduler

  override def down(node: Address): Unit = {
    if (isOldestNode(oldestMemberRole)) {
      probe ! DownCalled(node)
    } else if (isSecondaryOldest(oldestMemberRole)) {
      probe ! DownCalledBySecondaryOldest(node)
    } else {
      probe ! DownNode(node, selfAddress)
    }
  }

  override def preStart(): Unit = {}

  override def shutdownSelf(): Unit = {
    probe ! ShutDownCausedBySplitBrainResolver
  }
}

class OldestAutoDowningSpec extends TestKit(ActorSystem("OldestAutoDowningSpec")) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll  {

  private val memberA = TestMember(Address("akka.tcp", "sys", "first", 2552), Up, Set("master"))
  private val memberB = TestMember(Address("akka.tcp", "sys", "second", 2552), Up, Set("master"))
  private val memberC = TestMember(Address("akka.tcp", "sys", "third", 2552), Up, Set("slave"))
  private val memberD = TestMember(Address("akka.tcp", "sys", "forth", 2552), Up, Set("slave"))
  private val memberE = TestMember(Address("akka.tcp", "sys", "fifth", 2552), Up, Set("slave"))

  private val membersWithDowningRoleByAge: immutable.SortedSet[Member] =
    immutable.SortedSet(memberA, memberB)(Member.ageOrdering)
  private val membersWithOtherRolesByAge: immutable.SortedSet[Member] =
    immutable.SortedSet(memberC, memberD, memberE)(Member.ageOrdering)
  private val initialMembersByAge: immutable.SortedSet[Member] =
    immutable.SortedSet(memberA, memberB, memberC, memberD, memberE)(Member.ageOrdering)
  private val oldestDowningProvider: ActorRef = system.actorOf(Props(
    new OldestAutoDownTestActor(
      initialMembersByAge.head.address, 1.seconds, None, testActor
    )))
  private val someMember = initialMembersByAge.drop(3).head
  private val secondOldestMember = initialMembersByAge.drop(1).head
  private val oldestMemberRole = Some("master")
  private val oldestMasterDowningProvider: ActorRef = system.actorOf(Props(
    new OldestAutoDownTestActor(
      membersWithDowningRoleByAge.head.address, 1.seconds, oldestMemberRole, testActor
    )))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  describe("oldest downing provider (with specific role) should correctly " +
    "handle cluster events for other nodes") {
    it("should down unreachable node when the oldest") {
      oldestMasterDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestMasterDowningProvider ! UnreachableMember(memberC)
      expectMsg(DownCalled(memberC.address))
    }

    it("should not down unreachable if not the oldest node (with downing role)") {
      val othersNodeDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          membersWithDowningRoleByAge.tail.head.address, 1.seconds, oldestMemberRole, testActor
        )))
      othersNodeDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      othersNodeDowningProvider ! UnreachableMember(membersWithOtherRolesByAge.head)
      expectNoMsg(3.seconds)
    }

    it("should not down unreachable if not the oldest node (with some other role)") {
      val othersNodeDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          membersWithOtherRolesByAge.head.address, 1.seconds, oldestMemberRole, testActor
        )))
      othersNodeDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      othersNodeDowningProvider ! UnreachableMember(membersWithOtherRolesByAge.tail.head)
      expectNoMsg(3.seconds)
    }

    it("if oldest is removed, second oldest should become oldest and remove nodes") {
      val secondOldestDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          membersWithDowningRoleByAge.drop(1).head.address, 1.seconds, oldestMemberRole, testActor
        )))
      secondOldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      secondOldestDowningProvider ! MemberRemoved(membersWithDowningRoleByAge.head.copy(Removed), Leaving)
      secondOldestDowningProvider ! UnreachableMember(membersWithOtherRolesByAge.head)
      expectMsg(DownCalled(membersWithOtherRolesByAge.head.address))
    }

    it("if second oldest is removed, oldest should continue to work") {
      oldestMasterDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestMasterDowningProvider ! UnreachableMember(membersWithDowningRoleByAge.drop(1).head)
      expectMsg(DownCalled(membersWithDowningRoleByAge.drop(1).head.address))
    }

    it("should down nodes after specified timeout") {
      val oldestDowningProviderWithTimeout = system.actorOf(Props(
        new OldestAutoDownTestActor(
          membersWithDowningRoleByAge.head.address, 3.seconds, oldestMemberRole, testActor
        )))
      oldestDowningProviderWithTimeout ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProviderWithTimeout ! UnreachableMember(membersWithOtherRolesByAge.head)
      expectNoMsg(3.seconds)
      expectMsg(DownCalled(membersWithOtherRolesByAge.head.address))
    }

    it("if last node with chosen role removed, just oldest node should take over") {
      val oldestWithoutSelectedRole = system.actorOf(Props(
        new OldestAutoDownTestActor(
          membersWithOtherRolesByAge.head.address, 1.seconds, oldestMemberRole, testActor
        )))
      oldestWithoutSelectedRole ! CurrentClusterState(members = initialMembersByAge)
      oldestWithoutSelectedRole ! MemberRemoved(membersWithDowningRoleByAge.head.copy(Removed), Leaving)
      oldestWithoutSelectedRole ! MemberRemoved(
        membersWithDowningRoleByAge.drop(1).head.copy(Removed), Leaving)
      oldestWithoutSelectedRole ! UnreachableMember(membersWithOtherRolesByAge.tail.head)
      expectMsg(
        DownNode(membersWithOtherRolesByAge.tail.head.address, membersWithOtherRolesByAge.head.address))
    }

    it("oldest without role should remove last oldest node with role if it becomes unreachable") {
      val oldestWithoutSelectedRole = system.actorOf(Props(
        new OldestAutoDownTestActor(
          membersWithOtherRolesByAge.head.address, 1.seconds, oldestMemberRole, testActor
        )))
      oldestWithoutSelectedRole ! CurrentClusterState(members = initialMembersByAge)
      oldestWithoutSelectedRole ! MemberRemoved(membersWithDowningRoleByAge.head.copy(Removed), Leaving)
      oldestWithoutSelectedRole ! UnreachableMember(membersWithDowningRoleByAge.drop(1).head)
      expectMsg(DownCalledBySecondaryOldest(membersWithDowningRoleByAge.drop(1).head.address))
    }

    it("should not down the node if not the oldest and oldest is alone unreachable") {
      val someOtherNode = system.actorOf(Props(
        new OldestAutoDownTestActor(
          membersWithOtherRolesByAge.head.address, 1.seconds, oldestMemberRole, testActor
        )))
      someOtherNode ! CurrentClusterState(members = initialMembersByAge)
      someOtherNode ! UnreachableMember(membersWithDowningRoleByAge.head)
      expectNoMsg(3.seconds)
    }
  }

  describe("oldest downing provider should correctly handle cluster events for other nodes") {
    it("should down unreachable node when the oldest") {
      oldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProvider ! UnreachableMember(secondOldestMember)
      expectMsg(DownCalled(secondOldestMember.address))
    }

    it("should not down unreachable if not the oldest node") {
      val othersNodeDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          initialMembersByAge.drop(3).head.address, 1.seconds, None, testActor
        )))
      othersNodeDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      othersNodeDowningProvider ! UnreachableMember(secondOldestMember)
      expectNoMsg(3.seconds)
    }

    it("if oldest is removed, second oldest should become oldest and remove nodes") {
      val secondOldestDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          secondOldestMember.address, 1.seconds, None, testActor
        )))
      secondOldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      secondOldestDowningProvider ! MemberRemoved(initialMembersByAge.head.copy(Removed), Leaving)
      secondOldestDowningProvider ! UnreachableMember(someMember)
      expectMsg(DownCalled(someMember.address))
    }

    it("should down nodes after specified timeout") {
      val oldestDowningProviderWithTimeout = system.actorOf(Props(
        new OldestAutoDownTestActor(
          initialMembersByAge.head.address, 3.seconds, None, testActor
        )))
      oldestDowningProviderWithTimeout ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProviderWithTimeout ! UnreachableMember(secondOldestMember)
      expectNoMsg(3.seconds)
      expectMsg(DownCalled(secondOldestMember.address))
    }

    it("should down unreachable nodes even when someone is leaving cluster") {
      oldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProvider ! MemberLeft(someMember.copy(Leaving))
      oldestDowningProvider ! UnreachableMember(secondOldestMember)
      expectMsg(DownCalled(secondOldestMember.address))
    }

    it("should not down unreachable node if it's already in a state Down (not yet removed)") {
      oldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProvider ! UnreachableMember(someMember.copy(Down))
      expectNoMsg(3.seconds)
    }

    it("should not down another node even if previous was not yet removed from cluster") {
      oldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProvider ! UnreachableMember(someMember)
      expectMsg(DownCalled(someMember.address))
      oldestDowningProvider ! UnreachableMember(secondOldestMember)
      expectMsg(DownCalled(secondOldestMember.address))
    }

    it("should down several unreachable nodes if they become unreachable at the same time") {
      oldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProvider ! UnreachableMember(someMember)
      oldestDowningProvider ! UnreachableMember(secondOldestMember)
      oldestDowningProvider ! UnreachableMember(initialMembersByAge.last)
      expectMsgPF() {
        case DownCalled(address) =>
        case message => throw new Exception(s"Got unexpected message: $message")
      }
      oldestDowningProvider ! MemberRemoved(someMember.copy(Removed), Down)
      oldestDowningProvider ! MemberRemoved(secondOldestMember.copy(Removed), Down)
      for (_ <- 1 to 2) {
        expectMsgPF() {
          case DownCalled(address) =>
          case message => throw new Exception(s"Got unexpected message: $message")
        }
      }
    }


    it("should down node even if next planned UnreachableTimeout got cancelled") {
      // TODO: this test can be potentially flaky. Rewrite the tests without sleep.
      val oldestDowningProviderWithTimeout = system.actorOf(Props(
        new OldestAutoDownTestActor(
          initialMembersByAge.head.address, 2.seconds, None, testActor
        )))
      oldestDowningProviderWithTimeout ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProviderWithTimeout ! UnreachableMember(someMember)
      Thread.sleep(1200) // make some pause between 2 unreachable nodes
      oldestDowningProviderWithTimeout ! UnreachableMember(secondOldestMember)
      Thread.sleep(1000) // wait a bit so that timeout for the first node was already triggered
      oldestDowningProviderWithTimeout ! ReachableMember(secondOldestMember)
      expectMsg(DownCalled(someMember.address))
    }


    it("should down multiple unreachable nodes one after another (with timeout setting)") {
      val oldestDowningProviderWithTimeout = system.actorOf(Props(
        new OldestAutoDownTestActor(
          initialMembersByAge.head.address, 1.seconds, None, testActor
        )))
      oldestDowningProviderWithTimeout ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProviderWithTimeout ! UnreachableMember(someMember)
      oldestDowningProviderWithTimeout ! UnreachableMember(secondOldestMember)
      oldestDowningProviderWithTimeout ! UnreachableMember(initialMembersByAge.last)
      expectMsgPF() {
        case DownCalled(address) =>
        case message => throw new Exception(s"Got unexpected message: $message")
      }
      expectMsgPF() {
        case DownCalled(_) =>
        case message => throw new Exception(s"Got unexpected message: $message")
      }
      expectMsgPF() {
        case DownCalled(_) =>
        case message => throw new Exception(s"Got unexpected message: $message")
      }
    }

    it("should down unreachable nodes without waiting for nodes to be removed") {
      val clusterStateWithDownMember =
        (initialMembersByAge - secondOldestMember) + secondOldestMember.copy(Down)
      oldestDowningProvider ! CurrentClusterState(
        members = clusterStateWithDownMember,
        unreachable = Set(someMember)
      )
      expectMsg(DownCalled(someMember.address))
    }

    it("should not down a node if it becomes reachable within specified timeout") {
      val oldestDowningProviderWithLongerTimeout = system.actorOf(Props(
        new OldestAutoDownTestActor(
          initialMembersByAge.head.address, 4.seconds, None, testActor
        )))
      oldestDowningProviderWithLongerTimeout ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProviderWithLongerTimeout ! UnreachableMember(secondOldestMember)
      expectNoMsg(2.seconds)
      oldestDowningProviderWithLongerTimeout ! ReachableMember(secondOldestMember)
      expectNoMsg(3.seconds)
    }

    it("should not do anything if unreacheable member was removed during waiting for timeout") {
      val oldestDowningProviderWithTimeout = system.actorOf(Props(
        new OldestAutoDownTestActor(
          initialMembersByAge.head.address, 3.seconds, None, testActor
        )))
      oldestDowningProviderWithTimeout ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProviderWithTimeout ! UnreachableMember(secondOldestMember)
      oldestDowningProviderWithTimeout ! MemberRemoved(secondOldestMember.copy(Removed), Down)
      expectNoMsg(3.seconds)
    }

    it("should not down the node if not the oldest and oldest is alone unreachable") {
      val othersNodeDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          someMember.address, 1.seconds, None, testActor
        )))
      othersNodeDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      othersNodeDowningProvider ! UnreachableMember(initialMembersByAge.head)
      expectNoMsg(3.seconds)
    }

    it("should not down the node if not the oldest and oldest is alone unreachable even though there are" +
      " some nodes in Joining state") {
      val othersNodeDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          someMember.address, 1.seconds, None, testActor
        )))
      val joiningMember = TestMember(Address("akka.tcp", "sys", "joining", 2552), Joining, Set("slave"))
      othersNodeDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      othersNodeDowningProvider ! ReachableMember(joiningMember)
      othersNodeDowningProvider ! UnreachableMember(initialMembersByAge.head)
      expectNoMsg(3.seconds)
    }

    it("should down joining node if it becomes unreachable after a while") {
      val joiningMember = TestMember(Address("akka.tcp", "sys", "joining", 2552), Joining, Set("slave"))
      oldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      oldestDowningProvider ! ReachableMember(joiningMember)
      oldestDowningProvider ! UnreachableMember(joiningMember)
      expectMsg(DownCalled(joiningMember.address))
    }
  }

  describe("nodes should correctly shutdown itself if several nodes (with oldest) unreachable") {
    it("should shutdown self if partitioned from oldest node") {
      val someDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          someMember.address, 0.5.seconds, None, testActor
        )))
      someDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      someDowningProvider ! UnreachableMember(initialMembersByAge.head)
      someDowningProvider ! UnreachableMember(secondOldestMember)
      expectMsg(ShutDownCausedBySplitBrainResolver)
    }

    it("oldest node should shutdown itself if it's alone"){
      val oldestDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          initialMembersByAge.head.address, 0.5.seconds, None, testActor
        )))
      oldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      for (clusterMember <- initialMembersByAge.tail) {
        oldestDowningProvider ! UnreachableMember(clusterMember)
      }
      expectMsg(ShutDownCausedBySplitBrainResolver)
    }

    it("second oldest should down oldest node if it's unreachable"){
      val secondOldestDowningProvider = system.actorOf(Props(
        new OldestAutoDownTestActor(
          secondOldestMember.address, 0.5.seconds, None, testActor
        )))
      secondOldestDowningProvider ! CurrentClusterState(members = initialMembersByAge)
      secondOldestDowningProvider ! UnreachableMember(initialMembersByAge.head)
      expectMsg(DownCalledBySecondaryOldest(initialMembersByAge.head.address))
    }
  }
}
