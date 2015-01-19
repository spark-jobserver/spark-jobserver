package ooyala.common.akka

import org.scalatest.{FunSpecLike, Matchers}
import akka.testkit.TestActorRef

import akka.actor.{Actor, ActorSystem}


class ActorMetricsSpec extends FunSpecLike with Matchers {
  implicit val system = ActorSystem("test")

  describe("actor metrics") {
    it("should increment receive count metric when a message is received") {
      val actorRef = TestActorRef(new DummyActor with ActorMetrics)
      val actor = actorRef.underlyingActor

      actorRef ! "me"
      actor.metricReceiveTimer.count should equal (1)
    }
  }
}
