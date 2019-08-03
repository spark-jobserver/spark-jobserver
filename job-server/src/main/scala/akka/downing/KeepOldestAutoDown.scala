/**
  * This code is a modified version of parts of "Akka-cluster-custom-downing" project:
  * https://github.com/TanUkkii007/akka-cluster-custom-downing
  */

package akka.downing

import akka.ConfigurationException
import akka.actor.{Actor, Address, Cancellable, Props, Scheduler}
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus._
import akka.cluster.{Member, MemberStatus, _}
import org.slf4j.LoggerFactory

import scala.collection.SortedSet
import scala.concurrent.Await
import scala.concurrent.duration._

object KeepOldestAutoDown {
  case class UnreachableTimeout(member: Member)

  def props(oldestMemberRole: Option[String], shutdownActorSystem: Boolean,
            autoDownUnreachableAfter: FiniteDuration): Props =
    Props(classOf[KeepOldestAutoDown], oldestMemberRole,
      shutdownActorSystem, autoDownUnreachableAfter)
}

class KeepOldestAutoDown(preferredOldestMemberRole: Option[String],
                         shutdownActorSystem: Boolean, autoDownUnreachableAfter: FiniteDuration)
  extends Actor {

  import KeepOldestAutoDown._
  import context.dispatcher

  if (autoDownUnreachableAfter == Duration.Zero) {
    throw new ConfigurationException(
      "Downing provider will down the oldest node if it's alone," +
        "so autoDownUnreachableAfter timeout must be greater than zero.")
  }

  protected var membersByAge: SortedSet[Member] = SortedSet.empty(Member.ageOrdering)
  private val cluster = Cluster(context.system)
  private val skipMemberStatus = Set[MemberStatus](Down, Exiting)
  private var scheduledUnreachable: Map[Member, Cancellable] = Map.empty // waiting for timeout to fire
  private var pendingUnreachable: Set[Member] = Set.empty // waiting for the oldest node to down these nodes
  // already timed out, should be downed as soon as all scheduled time out
  private var unstableUnreachable: Set[Member] = Set.empty
  private val logger = LoggerFactory.getLogger(getClass)

  def selfAddress: Address = cluster.selfAddress

  def scheduler: Scheduler = {
    if (context.system.scheduler.maxFrequency < 1.second / cluster.settings.SchedulerTickDuration) {
      logger.warn("CustomDowning does not use a cluster dedicated scheduler." +
        "Cluster will use a dedicated scheduler if configured " +
        "with 'akka.scheduler.tick-duration' [{} ms] >  'akka.cluster.scheduler.tick-duration' [{} ms].",
        (1000 / context.system.scheduler.maxFrequency).toInt, cluster.settings.SchedulerTickDuration.toMillis)
    }
    context.system.scheduler
  }

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[ClusterDomainEvent])
    super.preStart()
  }

  override def postStop(): Unit = {
    scheduledUnreachable.values foreach { _.cancel }
    cluster.unsubscribe(self)
  }

  def receive: Receive = {
    case state: CurrentClusterState =>
      logger.info(s"Got message state: $state")
      initialize(state)
      state.unreachable foreach scheduleUnreachable

    case UnreachableTimeout(member) =>
      if (scheduledUnreachable contains member) {
        scheduledUnreachable -= member
      } else {
        logger.info(s"UnreachableTimeout was triggered for $member, but member is not in the list" +
          s"of scheduled unreachable members: $scheduledUnreachable")
      }
      if (scheduledUnreachable.isEmpty) {
        unstableUnreachable += member
        logger.info(s"UnreachableTimeout was triggered for members: $unstableUnreachable. \n" +
          s"This is the current cluster state (sorted by age): $membersByAge. \n" +
          s"Current list of pending members (other node should have downed them): $pendingUnreachable")
        downUnreachableNodes(unstableUnreachable)
      } else {
        logger.info(s"Adding $member to unstableUnreachable: $unstableUnreachable")
        unstableUnreachable += member
      }

    case MemberUp(m) =>
      logger.info("{} is up", m)
      refreshMember(m)
    case UnreachableMember(m) =>
      logger.info("{} is unreachable", m)
      refreshMember(m)
      scheduleUnreachable(m)
    case ReachableMember(m) =>
      logger.info("{} is reachable", m)
      refreshMember(m)
      removeFromUnreachable(m)
    case MemberLeft(m) =>
      logger.info("{} is left the cluster", m)
      refreshMember(m)
    case MemberExited(m) =>
      logger.info("{} exited the cluster", m)
      refreshMember(m)
    case MemberRemoved(m, prev) =>
      logger.info("{} was removed from the cluster", m)
      removeFromUnreachable(m)
      removeMember(m)
      onMemberRemoved(m, prev)
    case anotherEvent: ClusterDomainEvent => logger.debug(s"Ignoring akka cluster event: $anotherEvent")
  }

  def initialize(state: CurrentClusterState): Unit = {
    logger.info(s"Initialize called with state: $state")
    membersByAge = SortedSet.empty(Member.ageOrdering) union state.members.filterNot {m =>
      m.status == MemberStatus.Removed
    }
    logger.info(s"After initialize there are following membersByAge: $membersByAge")
  }

  def down(node: Address): Unit = {
    logger.info("Oldest is auto-downing unreachable node [{}]", node)
    cluster.down(node)
  }

  def shutdownSelf(): Unit = {
    if (shutdownActorSystem) {
      logger.info("ShutdownSelf: shutting down the system")
      Await.result(context.system.terminate(), 10 seconds)
    } else {
      logger.info("ShutdownSelf: exiting the system with -1")
      Runtime.getRuntime.halt(-1)
    }
  }

  def onMemberRemoved(member: Member, previousStatus: MemberStatus): Unit = {
    if (isOldestNode(preferredOldestMemberRole)) {
      logger.info("Oldest node in the cluster. Downing pending unreachable members.")
      downPendingUnreachableMembers()
    } else {
      logger.debug("Still not the oldest node, not making any cleanup")
    }
  }

  def downUnreachableNodes(members: Set[Member]): Unit = {
    // Till the moment there are nodes with roles in the cluster -> use selected role
    // Once all of them are removed -> start selecting just the oldest node
    // It is done for the case there is no jobserver, but cluster continues to function
    // Jobserver won't be able to join slaves, if their cluster is unhealthy
    val currentRoleToUse = if (membersWithRole(preferredOldestMemberRole).isEmpty) {
      None
    } else {
      preferredOldestMemberRole
    }
    logger.info(s"Currently used role for the oldest node: $currentRoleToUse")
    val oldest = oldestMember(currentRoleToUse)
    if (isOldestAlone(currentRoleToUse)) {
      if (isOldestNode(currentRoleToUse)) {
        logger.info("I am oldest node and alone, shutdown myself.")
        shutdownSelf()
      } else if (isSecondaryOldest(currentRoleToUse)) {
        logger.info("Downing on secondary oldest node")
        downUnreachableNodes()
      } else {
        logger.info("Not the oldest/secondary node. Adding pending unreachable members (oldest is alone)")
        pendingUnreachable ++= members
      }
    } else {
      if (oldest.fold(true)(o => members.contains(o))) {
        logger.info("OldestNode is unreachable. Trigger shutdown myself.")
        shutdownSelf()
      } else {
        if (isOldestNode(currentRoleToUse)) {
          logger.info("Oldest node is downing all unreachable members.")
          downUnreachableNodes()
        } else {
          logger.info("Not the oldest node - pending unreachable members")
          pendingUnreachable ++= members
        }
      }
    }
  }

  def refreshMember(member: Member): Unit = {
    logger.info(s"Replacing member: $member")
    membersByAge -= member
    membersByAge += member
    logger.debug(s"New membersByAge: $membersByAge")
  }

  def removeMember(member: Member): Unit = {
    logger.info(s"Removing member: $member")
    membersByAge -= member
    logger.debug(s"New membersByAge: $membersByAge")
  }

  def isOldestNode(role: Option[String]): Boolean = {
    val isOldest = membersWithRole(role).headOption.map(_.address).contains(selfAddress)
    logger.debug(s"Oldest node is unsafe (should take action): $isOldest")
    isOldest
  }

  def isOldestAlone(role: Option[String]): Boolean = {
    val isOldestAlone = if (membersByAge.size <= 1) {
      true
    }
    else {
      val oldest = oldestMember(role).get
      val rest = membersByAge.filter(_ != oldest)
      if (isOldestNode(role)) {
        isOK(oldest) && rest.forall(isNotOK)
      } else {
        isNotOK(oldest) && rest.forall(isOK)
      }
    }
    logger.info(s"Oldest node is alone: $isOldestAlone")
    isOldestAlone
  }

  def isSecondaryOldest(role: Option[String]): Boolean = {
    val tm = membersWithRole(role)
    val isSecondaryOldest = if (tm.size >= 2) {
      tm.slice(1, 2).head.address == selfAddress
    } else if (tm.size == 1 && membersByAge.size > 1) {
      logger.info(s"There is no second oldest with selected role. Choosing oldest from the rest.")
      val oldest = oldestMember(role).get
      membersByAge.filter(_ != oldest).head.address == selfAddress
    }
    else {
      false
    }
    logger.info(s"SecondaryOldest: $isSecondaryOldest")
    isSecondaryOldest
  }

  def oldestMember(role: Option[String]): Option[Member] = membersWithRole(role).headOption

  private def membersWithRole(role: Option[String]): SortedSet[Member] = {
    role.fold(membersByAge)(r => membersByAge.filter(_.hasRole(r)))
  }

  private def isOK(member: Member): Boolean = {
    (member.status == Up || member.status == Leaving || member.status == Joining) &&
      (!pendingUnreachable.contains(member) && !unstableUnreachable.contains(member))
  }

  private def isNotOK(member: Member): Boolean = !isOK(member)

  def scheduleUnreachable(m: Member): Unit =
    if (!skipMemberStatus(m.status) && !scheduledUnreachable.contains(m)) {
      logger.info(s"Schedule unreachable timeout($autoDownUnreachableAfter) for member $m")
      val task = scheduler.scheduleOnce(autoDownUnreachableAfter, self, UnreachableTimeout(m))
      scheduledUnreachable += (m -> task)
    }

  def removeFromUnreachable(member: Member): Unit = {
    scheduledUnreachable.get(member) foreach { _.cancel }
    scheduledUnreachable -= member
    pendingUnreachable -= member
    unstableUnreachable -= member
  }

  def downPendingUnreachableMembers(): Unit = {
    logger.info(s"Current pendingUnreachable (in downPendingUnreachableMembers): $pendingUnreachable")
    pendingUnreachable.foreach(member => down(member.address))
    pendingUnreachable = Set.empty
  }

  def downUnreachableNodes(): Unit = {
    logger.info(s"Current unstableUnreachable (in downUnreachableNodes): $unstableUnreachable")
    unstableUnreachable.foreach(member => down(member.address))
    unstableUnreachable = Set.empty
  }
}
