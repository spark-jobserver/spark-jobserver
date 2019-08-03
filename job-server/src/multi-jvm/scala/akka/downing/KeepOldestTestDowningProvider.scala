package akka.downing

import akka.actor.{ActorRef, ActorSystem, Address, Props}
import akka.cluster.Member
import akka.downing.KeepOldestTestAutoDown._

import scala.collection.SortedSet
import scala.concurrent.duration.FiniteDuration

class KeepOldestTestDowningProvider(system: ActorSystem) extends  KeepOldestDowningProvider(system) {
  override def downingActorProps: Option[Props] = {
    val (stableAfter: FiniteDuration,
    preferredOldestMemberRole: Option[_root_.java.lang.String],
    shutdownActorSystem: Boolean) = getDowningProviderParams

    Some(KeepOldestTestAutoDown.props(preferredOldestMemberRole, shutdownActorSystem, stableAfter))
  }
}

object KeepOldestTestAutoDown {
  object HealthCheck
  object HealthCheckPassed
  object InitializeTestActorRef
  object PingTestActor
  object ShutDownSelfTriggered
  object GetMembersByAge
  case class MembersByAge(members: SortedSet[Member])
  case class ShutDownAnotherNode(node: Address)

  def props(oldestMemberRole: Option[String], shutdownActorSystem: Boolean,
            autoDownUnreachableAfter: FiniteDuration): Props =
    Props(classOf[KeepOldestTestAutoDown], oldestMemberRole,
      shutdownActorSystem, autoDownUnreachableAfter)
}

class KeepOldestTestAutoDown(preferredOldestMemberRole: Option[String],
                             shutdownActorSystem: Boolean, autoDownUnreachableAfter: FiniteDuration)
  extends KeepOldestAutoDown(preferredOldestMemberRole, shutdownActorSystem, autoDownUnreachableAfter) {

  var testActorRef: ActorRef = _

  override def receive: Receive = super.receive.orElse({
    case InitializeTestActorRef =>
      testActorRef = sender()
      testActorRef ! PingTestActor
    case GetMembersByAge => sender() ! MembersByAge(membersByAge)
  })

  override def shutdownSelf(): Unit = {
    testActorRef ! ShutDownSelfTriggered
  }

  override  def down(node: Address): Unit = {
    testActorRef! ShutDownAnotherNode(node)
  }
}