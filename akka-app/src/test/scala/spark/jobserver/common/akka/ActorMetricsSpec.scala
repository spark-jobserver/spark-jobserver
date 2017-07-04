package spark.jobserver.common.akka

import org.scalatest.{BeforeAndAfterAll, Matchers, FunSpec}
import akka.testkit.{TestKit, TestActorRef}

import akka.actor.{Actor, ActorSystem}


class ActorMetricsSpec extends FunSpec with Matchers with BeforeAndAfterAll {
  implicit val system = ActorSystem("test")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  describe("actor metrics") {
    it("should increment receive count metric when a message is received") {
      val actorRef = TestActorRef(new DummyActor with ActorMetrics)
      val actor = actorRef.underlyingActor

      actorRef ! "me"
      actor.metricReceiveTimer.count should equal (1)
    }
  }
}
