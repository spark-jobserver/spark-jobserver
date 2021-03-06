package spark.jobserver.common.akka.web

import java.util.concurrent.TimeUnit

import akka.testkit.TestKit
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.OK
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CommonRoutesSpec extends AnyFunSpec with Matchers with ScalatestRouteTest with CommonRoutes {
  def actorRefFactory: ActorSystem = system

  val metricCounter = Metrics.newCounter(getClass, "test-counter")
  val metricMeter = Metrics.newMeter(getClass, "test-meter", "requests", TimeUnit.SECONDS)
  val metricHistogram = Metrics.newHistogram(getClass, "test-hist")
  val metricTimer = Metrics.newTimer(getClass, "test-timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val metricGauge = Metrics.newGauge(getClass, "test-gauge", new Gauge[Int] {
    def value() = 10
  })

  val counterMap = Map("type" -> "counter", "count" -> 0)
  val gaugeMap = Map("type" -> "gauge", "value" -> 10)

  val meterMap = Map("type" -> "meter", "units" -> "seconds", "count" -> 0, "mean" -> 0.0,
    "m1" -> 0.0, "m5" -> 0.0, "m15" -> 0.0)
  val histMap = Map("type" -> "histogram", "median" -> 0.0, "p75" -> 0.0, "p95" -> 0.0,
    "p98" -> 0.0, "p99" -> 0.0, "p999" -> 0.0)
  val timerMap = Map("type" -> "timer", "rate" -> (meterMap - "type"),
    "duration" -> (histMap ++ Map("units" -> "milliseconds") ++ Map("mean" -> 0.0) - "type"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  describe("/metricz route") {
    it("should report all metrics") {
      Get("/metricz") ~> commonRoutes ~> check {
        status === OK

        val metricsMap = JsonUtils.mapFromJson(responseAs[String])
        val classMetrics = metricsMap(getClass.getName).asInstanceOf[Map[String, Any]]

        classMetrics.keys.toSet should equal (
          Set("test-counter", "test-meter", "test-hist", "test-timer", "test-gauge")
        )
        classMetrics("test-counter") should equal (counterMap)
        classMetrics("test-meter") should equal (meterMap)
        classMetrics("test-hist") should equal (histMap)
        classMetrics("test-timer") should equal (timerMap)
      }
    }
  }

  describe("metrics serializer") {
    it("should serialize all metrics") {
      val flattenedMap = MetricsSerializer.asFlatMap()

      List("test-meter", "test-counter", "test-timer", "test-gauge", "test-hist") foreach { metricName =>
        flattenedMap.keys should contain("spark.jobserver.common.akka.web.CommonRoutesSpec." + metricName)
      }

      flattenedMap("spark.jobserver.common.akka.web.CommonRoutesSpec.test-meter") should equal(meterMap)
      flattenedMap("spark.jobserver.common.akka.web.CommonRoutesSpec.test-counter") should equal(counterMap)
      flattenedMap("spark.jobserver.common.akka.web.CommonRoutesSpec.test-hist") should equal(histMap)
      flattenedMap("spark.jobserver.common.akka.web.CommonRoutesSpec.test-timer") should equal(timerMap)
      flattenedMap("spark.jobserver.common.akka.web.CommonRoutesSpec.test-gauge") should equal(gaugeMap)
    }
  }
}
