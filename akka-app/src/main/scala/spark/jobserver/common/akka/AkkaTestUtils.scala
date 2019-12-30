package spark.jobserver.common.akka

import akka.actor.{ActorSystem, ActorRef}
import akka.pattern.gracefulStop
import scala.concurrent.Await

object AkkaTestUtils {
  import scala.concurrent.duration._

  private val timeout = 15 seconds

  def shutdownAndWait(actor: ActorRef) {
    if (actor != null) {
      val stopped = gracefulStop(actor, timeout)
      Await.result(stopped, timeout + (1 seconds))
    }
  }

  def shutdownAndWait(system: ActorSystem) {
    if (system != null) {
      system.terminate()
      Await.result(system.whenTerminated, timeout)
    }
  }
}
