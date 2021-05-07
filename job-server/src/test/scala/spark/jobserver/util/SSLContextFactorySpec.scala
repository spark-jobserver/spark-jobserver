package spark.jobserver.util

import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import org.apache.spark.SparkConf
import java.io.FileNotFoundException
import javax.net.ssl.SSLContext
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SSLContextFactorySpec extends AnyFunSpec with Matchers {
  import collection.JavaConverters._

  val config = ConfigFactory.parseMap(Map(
    "akka.http.server.ssl-encryption" -> "on",
    "akka.http.server.keystore" -> "~/sjs.jks",
    "akka.http.server.keystorePW" -> "changeit",
    "akka.http.server.encryptionType" -> "SSL", //SSL or TLS
    "akka.http.server.keystoreType" -> "JKS",
    "akka.http.server.provider" -> "SunX509").asJava).withValue("akka.http.server.enabledProtocols", ConfigValueFactory.fromIterable(List("SSLv3", "TLSv1").asJava))

  describe("SSLContextFactory.checkRequiredParamsSet") {
    it("should throw an exception when keystore param is missing") {
      val thrown = the[RuntimeException] thrownBy SSLContextFactory.checkRequiredParamsSet(config.getConfig("akka.http.server").withoutPath("keystore"))
      thrown.getMessage should equal(SSLContextFactory.MISSING_KEYSTORE_MSG)
    }

    it("should throw an exception when keystore password param is missing") {
      val thrown = the[RuntimeException] thrownBy SSLContextFactory.checkRequiredParamsSet(config.getConfig("akka.http.server").withoutPath("keystorePW"))
      thrown.getMessage should equal(SSLContextFactory.MISSING_KEYSTORE_PASSWORD_MSG)
    }

    it("should be silent when all necessary params are set") {
      SSLContextFactory.checkRequiredParamsSet(config.getConfig("akka.http.server"))
    }
  }

  describe("SSLContextFactory.createContext") {
    it("should throw an exception when keystore param is missing") {
      val thrown = the[RuntimeException] thrownBy SSLContextFactory.createContext(config.getConfig("akka.http.server").withoutPath("keystore"))
      thrown.getMessage should equal(SSLContextFactory.MISSING_KEYSTORE_MSG)
    }

    it("should throw an exception when keystore password param is missing") {
      val thrown = the[RuntimeException] thrownBy SSLContextFactory.createContext(config.getConfig("akka.http.server").withoutPath("keystorePW"))
      thrown.getMessage should equal(SSLContextFactory.MISSING_KEYSTORE_PASSWORD_MSG)
    }

    it("should throw an exception when keystore file cannot be found") {
      an[FileNotFoundException] should be thrownBy SSLContextFactory.createContext(config.getConfig("akka.http.server"))
    }

    it("should throw an exception when keystore file cannot be read") {
      val f = java.io.File.createTempFile("utgg", "jks")
      an[Exception] should be thrownBy
        SSLContextFactory.createContext(config.getConfig("akka.http.server").
          withValue("keystore", ConfigValueFactory.fromAnyRef(f.getAbsolutePath)))
    }

    it("should create default context when encryption is off") {
      val defaultContext = SSLContextFactory.createContext(config.getConfig("akka.http.server").
          withValue("ssl-encryption", ConfigValueFactory.fromAnyRef("off")))
      defaultContext should be (SSLContext.getDefault)
    }
  }

}
