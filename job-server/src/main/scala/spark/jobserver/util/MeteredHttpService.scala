package spark.jobserver.util

import akka.http.scaladsl.server.{Directive0, Directives}
import spark.jobserver.common.akka.metrics.YammerMetrics

trait MeteredHttpService extends YammerMetrics {

  private val totalReadRequests = counter("total-read-requests")
  private val totalWriteRequests = counter("total-write-requests")

  /*
   * Count read requests
   */

  def get : Directive0 = {
    totalReadRequests.inc()
    Directives.get
  }

  def head : Directive0 = {
    totalReadRequests.inc()
    Directives.head
  }

  def options : Directive0 = {
    totalReadRequests.inc()
    Directives.options
  }

  /*
   * Count write requests
   */

  def post : Directive0 = {
    totalWriteRequests.inc()
    Directives.post
  }

  def delete : Directive0 = {
    totalWriteRequests.inc()
    Directives.delete
  }

  def put : Directive0 = {
    totalWriteRequests.inc()
    Directives.put
  }

  def patch : Directive0 = {
    totalWriteRequests.inc()
    Directives.patch
  }

}