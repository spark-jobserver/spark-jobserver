package spark.jobserver

import akka.actor.{Props, ActorSystem, ActorRef}
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{FunSpec, BeforeAndAfter, BeforeAndAfterAll}



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
      actor ! NotificationActor.JobNotification("invalid-id", "SUCCESS", Some("http://httpbin.org/get"))
      expectNoMsg()
    }

    it("should accept the JobNotification message without callback url"){
      actor ! NotificationActor.JobNotification("invalid-id", "SUCCESS", None)
      expectNoMsg()
    }


  }

}

