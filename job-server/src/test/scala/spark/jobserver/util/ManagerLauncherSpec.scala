package spark.jobserver.util

import org.scalatest.{ Matchers, FunSpec, BeforeAndAfter}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.collections.map.MultiValueMap
import scala.collection.mutable.HashMap
import collection.JavaConverters._
import java.io.{File, IOException}
import org.apache.spark.launcher.{SparkLauncher, SparkAppHandle}

class ManagerLauncherSpec extends FunSpec with Matchers with BeforeAndAfter {
    val stubbedSparkLauncher = new StubbedSparkLauncher()
    val environment = new InMemoryEnvironment

    lazy val baseSystemConf = buildConfig(Map("spark.master" -> "local[*]", "spark.submit.deployMode" -> "client"))
    lazy val baseContextMap = Map("launcher.spark.driver.memory" -> "1g")
    lazy val baseContextConf = buildConfig(baseContextMap)
    lazy val managerLauncherFunc = new ManagerLauncher(baseSystemConf, _:Config, "", "", "", stubbedSparkLauncher, environment)

    def resetStoredSettings() {
      environment.clear()
      stubbedSparkLauncher.clear()
    }

    lazy val tempJarFile = {
      val tempFile = File.createTempFile("dummy-test", ".jar")
      tempFile.deleteOnExit()
      tempFile.getAbsolutePath
    }

    def buildConfig(map: Map[String, String]) : Config = {
      ConfigFactory.parseMap(map.asJava).withFallback(ConfigFactory.defaultOverrides())
    }

    before {
      resetStoredSettings()
      environment.set("MANAGER_JAR_FILE", tempJarFile)
    }

    describe("ManagerLauncher black box tests") {
      it("should fail if sjs jar path is wrong or empty") {
        environment.set("MANAGER_JAR_FILE", "/wrong/path")

        val launcher = managerLauncherFunc(baseContextConf)
        launcher.start()._1 should be (false)
      }

     it("should pass if sjs jar path is valid") {
        val launcher = managerLauncherFunc(baseContextConf)

        launcher.start()._1 should be (true)
      }

     it("should fail if wrong driver memory is provided") {
       val contextConfMap = Map("launcher.spark.driver.memory" -> "1gb")

       val launcher = managerLauncherFunc(buildConfig(contextConfMap))
       launcher.start()._1 should be (false)

       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.driver.memory=1gb") should be (false)
     }

     it("should pass if memory is provided without unit (for bytes)") {
       val contextConfMap = Map("launcher.spark.driver.memory" -> "1000000")
       val launcher = managerLauncherFunc(buildConfig(contextConfMap))

       launcher.start()._1 should be (true)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf","spark.driver.memory=1000000") should be (true)
     }

     it("should pass any spark configuration specified in launcher section to SparkLauncher") {
       val contextConfMap = baseContextMap + ("launcher.test.spark.config" -> "dummy")

       val launcher = managerLauncherFunc(buildConfig(contextConfMap))

       launcher.start()._1 should be (true)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.driver.memory=1g") should be (true)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "test.spark.config=dummy") should be (true)
     }

     it("should pass spark.proxy.user to SparkLauncher if specified") {
       val contextConfMap = baseContextMap + ("spark.proxy.user" -> "proxy-setting")

       val launcher = managerLauncherFunc(buildConfig(contextConfMap))
       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue("--proxy-user", "proxy-setting") should be (true)
     }

     it("should pass spark configurations set through environment variable to SparkLauncher") {
       environment.set("MANAGER_EXTRA_SPARK_CONFS", "spark.yarn.submit.waitAppCompletion=false|spark.files=dummy-file")

       val launcher = managerLauncherFunc(baseContextConf)
       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.yarn.submit.waitAppCompletion=false") should be (true)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.files=dummy-file") should be (true)
     }

     it("should allow launcher.spark.master to override spark.master") {
       val contextConfMap = baseContextMap + ("launcher.spark.master" -> "new-master")

       val launcher = managerLauncherFunc(buildConfig(contextConfMap))
       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.SPARK_MASTER, "new-master") should be (true)
     }

     it("should pass all application arguments to SparkLauncher") {
       environment.set("MANAGER_CONF_FILE", "conf-file")

       val managerLauncher = new ManagerLauncher(baseSystemConf, baseContextConf, "master-address", "actor-name", "", stubbedSparkLauncher, environment)
       managerLauncher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.APP_ARGS, "master-address,actor-name,conf-file") should be (true)
     }
   }
}

class InMemoryEnvironment extends Environment {
    val envVariables = HashMap.empty[String, String]

    def get(key: String, default: String): String = {
      envVariables.getOrElse(key, default)
    }

    def set(key: String, value: String) {
      envVariables(key) = (value)
    }

    def clear() {
      envVariables.clear()
    }
}

object StubbedSparkLauncher {
    final val SPARK_HOME = "fake.spark.home"
    final val SPARK_MASTER = "fake.spark.master"
    final val DEPLOY_MODE = "fake.spark.submit.deployMode"
    final val APP_RESOURCES = "fake.spark.jar.path"
    final val MAIN_CLASS = "fake.spark.main.class"
    final val APP_ARGS = "fake.spark.app.args"
    final val SPARK_ARGS = "fake.spark.conf"
}

class StubbedSparkLauncher extends SparkLauncher {
      private val launcherConfig = new MultiValueMap()

      def getLauncherConfig(): MultiValueMap = {
        launcherConfig
      }

      def clear() {
        launcherConfig.clear()
      }

      override def setSparkHome(home: String): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.SPARK_HOME, home)
        null
      }

      override def setMaster(master: String): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.SPARK_MASTER, master)
        null
      }

      override def setDeployMode(mode: String): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.DEPLOY_MODE, mode)
        null
      }

      override def setAppResource(jarPath: String): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.APP_RESOURCES, jarPath)
        null
      }

      override def setMainClass(mainClass: String): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.MAIN_CLASS, mainClass)
        null
      }

      override def addAppArgs(args: String*): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.APP_ARGS, args.mkString(","))
        null
      }

      override def addSparkArg(key: String, value: String): SparkLauncher = {
        launcherConfig.put(key, value)
        null
      }

      override def startApplication(listeners: SparkAppHandle.Listener*): SparkAppHandle = {
        // Don't do anything
        null
      }
    }

