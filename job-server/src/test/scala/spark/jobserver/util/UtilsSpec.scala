package spark.jobserver.util

import akka.actor.Address
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import spark.jobserver.JobServer
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFunSpecLike with Matchers
  with BeforeAndAfter {

  describe("getHASeedNodes tests") {
    it("should return empty list if ha configs are not specified") {
      Utils.getHASeedNodes(ConfigFactory.empty()) should be(List.empty)
    }

    it("should return empty list if ha configs is empty") {
      val config = ConfigFactory.parseString("spark.jobserver.ha.masters=[]")
      Utils.getHASeedNodes(config) should be(List.empty)
    }

    it("should throw exception is wrong format is specified in configuration") {
      val config = ConfigFactory.parseString("spark.jobserver.ha.masters=[\"IP_WITHOUT_PORT\"]")
      val config1 = ConfigFactory.parseString("spark.jobserver.ha.masters=[\"IP:20\";\"IP2:20\"]")

      intercept[WrongFormatException] {
        Utils.getHASeedNodes(config)
      }

      intercept[WrongFormatException] {
        Utils.getHASeedNodes(config1)
      }
    }

    it("should return seed nodes") {
      val config = ConfigFactory.parseString("spark.jobserver.ha.masters=[\"IP:20\"]")
      Utils.getHASeedNodes(config) should be(
        List(Address("akka.tcp", JobServer.ACTOR_SYSTEM_NAME, "IP", 20)))
    }

    it("should return multiple seed nodes") {
      val config = ConfigFactory.parseString("spark.jobserver.ha.masters=[\"IP:20\", \"IP2:20\"]")
      Utils.getHASeedNodes(config) should be(
        List(Address("akka.tcp", JobServer.ACTOR_SYSTEM_NAME, "IP", 20),
             Address("akka.tcp", JobServer.ACTOR_SYSTEM_NAME, "IP2", 20)))
    }
  }
}
