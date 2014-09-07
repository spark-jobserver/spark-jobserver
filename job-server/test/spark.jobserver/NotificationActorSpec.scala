package spark.jobserver

import akka.actor.{Props, ActorSystem, ActorRef}
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{FunSpec, BeforeAndAfter, BeforeAndAfterAll}
import spark.jobserver.NotificationActor.{JobStatus, ContextStatus}


object NotificationSpec {
  val system = ActorSystem("test")
}


class NotificationSpec extends TestKit(NotificationSpec.system) with ImplicitSender
with FunSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  var actor: ActorRef = _

  before {
    actor = system.actorOf(Props(classOf[NotificationActor]))
  }


  override def afterAll() {
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(NotificationSpec.system)
  }


  describe("NotificationActor"){
    it("should accept the JobNotification message with callback url"){
      actor ! NotificationActor.JobNotification("invalid-id", JobStatus.SUCCESS, Some("http://httpbin.org/get"))
      expectNoMsg()
    }

    it("should accept the JobNotification message without callback url"){
      actor ! NotificationActor.JobNotification("invalid-id", JobStatus.SUCCESS, None)
      expectNoMsg()
    }

    it("should accept the ContextNotification message with callback url"){
      actor ! NotificationActor.ContextNotification("invalid-name", ContextStatus.INITIALIZED, Some("http://httpbin.org/get"))
      expectNoMsg()
    }

    it("should accept the ContextNotification message without callback url"){
      actor ! NotificationActor.ContextNotification("invalid-name", ContextStatus.INITIALIZED, None)
      expectNoMsg()
    }

  }

}

