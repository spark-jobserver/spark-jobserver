package spark.jobserver.metrics

import kamon.Kamon
import kamon.akka.http.KamonTraceDirectives
import kamon.akka.http.metrics.AkkaHttpServerMetrics

trait ServiceMetrics extends KamonTraceDirectives {
  Kamon.start()

  val metrics = Kamon.metrics.entity(AkkaHttpServerMetrics, "")

  sys.addShutdownHook {
    Kamon.shutdown()
  }
}
