package spark.jobserver

import akka.actor.ActorSystem
import spark.jobserver.io.JobInfo
import spark.jobserver.util.CallbackHandler

class CallbackTestsHelper extends CallbackHandler {

  var successCount = 0
  var failureCount = 0

  override def success(info: JobInfo, result: Any)(implicit system: ActorSystem): Unit =
    info.callbackUrl.foreach(_ => successCount += 1)

  override def failure(info: JobInfo, error: Throwable)(implicit system: ActorSystem): Unit =
    info.callbackUrl.foreach(_ => failureCount += 1)
}
