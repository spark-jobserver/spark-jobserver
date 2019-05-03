package spark.jobserver.util

import spray.routing.{HttpService, Directive0}
import spark.jobserver.common.akka.metrics.YammerMetrics

trait MeteredHttpService extends HttpService with YammerMetrics {

  private val totalReadRequests = counter("total-read-requests")
  private val totalWriteRequests = counter("total-write-requests")

  /*
   * Count read requests
   */

  override def get : Directive0 = {
    totalReadRequests.inc()
    super.get
  }

  override def head : Directive0 = {
    totalReadRequests.inc()
    super.head
  }

  override def options : Directive0 = {
    totalReadRequests.inc()
    super.options
  }

  /*
   * Count write requests
   */

  override def post : Directive0 = {
    totalWriteRequests.inc()
    super.post
  }

  override def delete : Directive0 = {
    totalWriteRequests.inc()
    super.delete
  }

  override def put : Directive0 = {
    totalWriteRequests.inc()
    super.put
  }

  override def patch : Directive0 = {
    totalWriteRequests.inc()
    super.patch
  }

}