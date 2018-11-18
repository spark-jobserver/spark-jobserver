package spark.jobserver.util

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}

import collection.JavaConverters._
import org.joda.time.DateTime

import scala.concurrent.duration._
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.io.ContextInfo

import scala.concurrent.Await
import scala.concurrent.duration.Duration


object ForcefulKillSpec {
  val PRIMARY_MASTER = 0
  val SECONDARY_MASTER = 1
}

class ForcefulKillSpec extends FunSpec with Matchers {

  implicit val system = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))

  def sparkUIJson(status: String = "ALIVE"): String =
    s"""
      |{
      |	"url": "spark://localhost:7077",
      |	"workers": [{
      |		"id": "worker-1",
      |		"state": "ALIVE"
      |	}],
      |	"cores": 8,
      |	"coresused": 0,
      |	"memory": 15360,
      |	"memoryused": 0,
      |	"activeapps": [],
      |	"completedapps": [],
      |	"activedrivers": [],
      |	"status": "$status"
      |}
    """.stripMargin

  def buildConfig(masterAddress: String) : Config = {
    val configMap = Map("spark.master" -> masterAddress)
    ConfigFactory.parseMap(configMap.asJava).withFallback(ConfigFactory.defaultOverrides())
  }

  val unusedContextInfo = ContextInfo("a", "a", "", None, DateTime.now(), None, "", None)

  describe("Spark standalone forceful UI kill") {
    it("should be able to kill the application") {
      createStubHelper("localhost:8080").kill()
    }

    it("should be able to handle IPs") {
      createStubHelper("127.0.0.1:8080").kill()
    }

    it("should be able to handle hostname with dashes") {
      createStubHelper("master-1:8080").kill()
    }

    it("should not try to kill if the master address does not conform to standalone master format") {
      val mesosMultiMaster = "mesos://zk://host1:2181,host2:2181,host3:2181/mesos"
      val helper = new StandaloneForcefulKill(_: Config, "app-test")

      intercept[NotStandaloneModeException](helper(buildConfig("local(*)")).kill())
      intercept[NotStandaloneModeException](helper(buildConfig("yarn")).kill())
      intercept[NotStandaloneModeException](helper(buildConfig("mesos://master-1:5050")).kill())
      intercept[NotStandaloneModeException](helper(buildConfig(mesosMultiMaster)).kill())
    }

    it("should handle gracefully if one or both masters are not in ALIVE state") {
      val helper: StandaloneForcefulKill = new StandaloneForcefulKill(buildConfig("spark://master1:8080,master2:8080"), "app-test") {
        var currentMaster: Int = ForcefulKillSpec.PRIMARY_MASTER
        override def getHTTPResponse(req: HttpRequest, timout: Duration = 30 seconds)(implicit system: ActorSystem = null): HttpResponse = {
          req.method match {
            case HttpMethods.GET =>
              currentMaster match {
                case ForcefulKillSpec.PRIMARY_MASTER =>
                  currentMaster = ForcefulKillSpec.SECONDARY_MASTER
                  req.uri.toString() should be("http://master1:8080/json/")
                  HttpResponse(entity = HttpEntity(sparkUIJson("STANDBY")))
                case ForcefulKillSpec.SECONDARY_MASTER =>
                  req.uri.toString() should be("http://master2:8080/json/")
                  HttpResponse(entity = HttpEntity(sparkUIJson("RECOVERING")))
              }
            case HttpMethods.POST => fail("Request should not be sent")
            case m => throw new Exception(s"unsupported HTTP METHOD: $m")
          }
        }
      }

      intercept[NoAliveMasterException](helper.kill())
    }

    it("should be able to kill the application if multiple masters are provided") {
      // Note: The code doesn't work. Keep on returning the first IP
      val helper: StandaloneForcefulKill = new StandaloneForcefulKill(buildConfig("spark://master1:8080,master2:8080"), "app-test") {
        var currentMaster: Int = ForcefulKillSpec.PRIMARY_MASTER
        override def getHTTPResponse(req: HttpRequest, timout: Duration = 30 seconds)(implicit system: ActorSystem = null): HttpResponse = {
          req.method match {
            case HttpMethods.GET =>
              currentMaster match {
                case ForcefulKillSpec.PRIMARY_MASTER =>
                  currentMaster = ForcefulKillSpec.SECONDARY_MASTER
                  req.uri.toString() should be("http://master1:8080/json/")
                  HttpResponse(entity = HttpEntity.apply(sparkUIJson("STANDBY")))
                case ForcefulKillSpec.SECONDARY_MASTER =>
                  req.uri.toString() should be("http://master2:8080/json/")
                  HttpResponse(entity = HttpEntity.apply(sparkUIJson("ALIVE")))
              }
            case HttpMethods.POST =>
              req.uri.toString() should be("http://master2:8080/app/kill/")
              val resString = Await.result(req.entity.dataBytes.map(_.utf8String).runFold("")((acc, i) => acc + i), Duration.Inf)
              resString should be("id=app-test&terminate=true")
              HttpResponse(StatusCodes.Found)
            case m => throw new Exception(s"unsupported HTTP METHOD: $m")
          }
        }
      }

      helper.kill()
    }
  }

  private def createStubHelper(masterAddressAndPort: String,
                               appId: String = "app-test",
                               failOnHTTPRequest: Boolean = false,
                               throwException: Boolean = false)(implicit materializer: ActorMaterializer): StandaloneForcefulKill = {
    val helper: StandaloneForcefulKill = new StandaloneForcefulKill(buildConfig(s"spark://$masterAddressAndPort"), appId) {
      override def getHTTPResponse(req: HttpRequest, timout: Duration = 30 seconds)(implicit system: ActorSystem = null): HttpResponse = {
        if (failOnHTTPRequest) fail("Request is not supposed to be sent")
        if (throwException) throw new Exception("deliberate failure")

        req.method match {
          case HttpMethods.GET =>
            req.uri.toString() should be(s"http://$masterAddressAndPort/json/")
            HttpResponse(entity = HttpEntity.apply(sparkUIJson()))
          case HttpMethods.POST =>
            req.uri.toString() should be(s"http://$masterAddressAndPort/app/kill/")
            val resString = Await.result(req.entity.dataBytes.map(_.utf8String).runFold("")((acc, i) => acc + i), Duration.Inf)
            resString should be("id=app-test&terminate=true")
            HttpResponse(status = StatusCodes.Found)
          case m => throw new Exception(s"unsupported HTTP METHOD: $m")
        }
      }
    }
    helper
  }
}