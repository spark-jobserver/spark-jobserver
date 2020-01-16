package spark.jobserver.util

import org.scalatest.{ Matchers, FunSpec, BeforeAndAfter}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.collections.map.MultiValueMap
import scala.collection.mutable.HashMap
import collection.JavaConverters._
import java.io.File
import org.apache.spark.launcher.{SparkLauncher, SparkAppHandle}

class ManagerLauncherSpec extends FunSpec with Matchers with BeforeAndAfter with HDFSCluster {
    val stubbedSparkLauncher = new StubbedSparkLauncher()
    val environment = new InMemoryEnvironment

    lazy val baseSystemConfMap = Map("spark.master" -> "local[*]", "spark.submit.deployMode" -> "client")
    lazy val baseSystemConf = buildConfig(baseSystemConfMap)
    lazy val baseContextMap = Map("launcher.spark.driver.memory" -> "1g")
    lazy val baseContextConf = buildConfig(baseContextMap)
    lazy val managerLauncherFunc = new ManagerLauncher(
      baseSystemConf, _: Config, "", "", "", "", stubbedSparkLauncher, environment)

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
        shouldFailOnWrongPath("file:///")
        shouldFailOnWrongPath("/wrong/path")
        shouldFailOnWrongPath("file:/wrong/path")
        shouldFailOnWrongPath("hdfs:/wrong/path")
        shouldFailOnWrongPath("hdfs://localhost:9020/wrong/path")

        def shouldFailOnWrongPath(path: String): Unit = {
          environment.set("MANAGER_JAR_FILE", path)
          val launcher = managerLauncherFunc(baseContextConf)
          val (hasStarted, _) = launcher.start()
          hasStarted should be(false)
        }
      }

     it("should pass if sjs jar path (local) is valid") {
        val launcher = managerLauncherFunc(baseContextConf)
        val (hasStarted, _) = launcher.start()
        hasStarted should be (true)
      }

      it("should accept valid HDFS path for jars") {
        super.startHDFS()
        val tempFile = File.createTempFile("hdfs-", ".jar")
        tempFile.deleteOnExit()
        val dummyHDFSDir = s"${super.getNameNodeURI()}/spark-jobserver"
        val dummyJarHDFSPath = s"${dummyHDFSDir}/${tempFile.getName}"

        super.writeFileToHDFS(tempFile.getAbsolutePath(), dummyHDFSDir)

        environment.set("MANAGER_JAR_FILE", dummyJarHDFSPath)
        val launcher = managerLauncherFunc(baseContextConf)
        val (hasStarted, _) = launcher.start()
        hasStarted should be(true)

        super.shutdownHDFS()
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

       val managerLauncher = new ManagerLauncher(baseSystemConf, baseContextConf, "master-address", "context-name", "actor-name", "", stubbedSparkLauncher, environment)
       managerLauncher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.APP_ARGS, "master-address,actor-name,conf-file") should be (true)
       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.APP_NAME, "context-name")
     }

     it("should fail if supervise mode is specified with client mode") {
       val baseConfWithSuperviseMode = baseSystemConfMap + ("spark.driver.supervise" -> "true")
       val launcher = new ManagerLauncher(buildConfig(baseConfWithSuperviseMode), baseContextConf, "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start() should be (false, "Supervise mode can only be used with cluster mode")
     }

      it("should fail if supervise mode is specified (in context config) with client mode") {
       val contextConfMap = baseContextMap + ("launcher.spark.driver.supervise" -> "true")

       val launcher = managerLauncherFunc(buildConfig(contextConfMap))

       launcher.start() should be (false, "Supervise mode can only be used with cluster mode")
     }

     it("should fail if supervise mode is specified with yarn") {
       val systemConfMap = Map("spark.master" -> "yarn", "spark.submit.deployMode" -> "cluster", "spark.driver.supervise" -> "true")

       val launcher = new ManagerLauncher(buildConfig(systemConfMap), baseContextConf, "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start() should be (false, "Supervise mode is only supported with spark standalone or Mesos")
     }

     it("should fail if supervise mode is specified (in context config) with yarn") {
       val systemConfMap = Map("spark.master" -> "yarn", "spark.submit.deployMode" -> "cluster")
       val contextConfMap = baseContextMap + (ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY -> "true")
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), buildConfig(contextConfMap), "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start() should be (false, "Supervise mode is only supported with spark standalone or Mesos")
     }

     it("should set supervise flag if system config and context config have supervise mode enabled") {
       val systemConfMap = Map("spark.master" -> "local[4]", "spark.submit.deployMode" -> "cluster", "spark.driver.supervise" -> "true")
       val contextConfMap = baseContextMap + (ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY -> "true")
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), buildConfig(contextConfMap), "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.SPARK_DRIVER_SUPERVISE, "--supervise") should be (true)
       // The supervise mode should not be added again while processing the launcher section
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.driver.supervise=true") should be (false)
     }

     it("should set supervise flag if only context config has supervise mode enabled") {
       val systemConfMap = Map("spark.master" -> "local[4]", "spark.submit.deployMode" -> "cluster")
       val contextConfMap = baseContextMap + (ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY -> "true")
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), buildConfig(contextConfMap), "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.SPARK_DRIVER_SUPERVISE, "--supervise") should be (true)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.driver.supervise=true") should be (false)
     }

     it("should not set supervise flag if supervise mode is enabled in system config but context config doesn't have a value defined") {
       val systemConfMap = Map("spark.master" -> "local[4]", "spark.submit.deployMode" -> "cluster", ManagerLauncher.SJS_SUPERVISE_MODE_KEY -> "true")
       val contextConfMap = baseContextMap
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), buildConfig(contextConfMap), "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.SPARK_DRIVER_SUPERVISE, "--supervise") should be (true)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.driver.supervise=true") should be (false)
     }

     it("should not set supervise flag if supervise mode is disabled in system config and context config doesn't have a value defined") {
       val systemConfMap = Map("spark.master" -> "local[4]", "spark.submit.deployMode" -> "cluster", ManagerLauncher.SJS_SUPERVISE_MODE_KEY -> "false")
       val contextConfMap = baseContextMap
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), buildConfig(contextConfMap), "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.SPARK_DRIVER_SUPERVISE, "--supervise") should be (false)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.driver.supervise=true") should be (false)
     }

     it("should not set supervise flag if supervise mode is enabled in system config but disabled in context config") {
       val systemConfMap = Map("spark.master" -> "local[4]", "spark.submit.deployMode" -> "cluster", ManagerLauncher.SJS_SUPERVISE_MODE_KEY -> "true")
       val contextConfMap = baseContextMap + (ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY -> "false")
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), buildConfig(contextConfMap), "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.SPARK_DRIVER_SUPERVISE, "--supervise") should be (false)
       stubbedSparkLauncher.getLauncherConfig().containsValue("--conf", "spark.driver.supervise=true") should be (false)
     }

     it("should not set supervise flag if both configs don't have supervise mode enabled") {
       val launcher = managerLauncherFunc(buildConfig(baseContextMap))

       launcher.start()

       stubbedSparkLauncher.getLauncherConfig().containsValue(StubbedSparkLauncher.SPARK_DRIVER_SUPERVISE, "--supervise") should be (false)
     }

     it("should write gc logs to current execution directory in cluster/supervise mode") {
       val systemConfMap = Map("spark.master" -> "yarn", "spark.submit.deployMode" -> "cluster")
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), baseContextConf, "", "", "", "", stubbedSparkLauncher, environment)

       launcher.start()

       val driverOptions = stubbedSparkLauncher.getLauncherConfig().get("--driver-java-options").asInstanceOf[java.util.ArrayList[String]]
       driverOptions.get(0).trim() should be("-Xloggc:gc.out")
     }

     it("should write gc logs to directory passed as argument (client mode scenario)") {
       val systemConfMap = Map("spark.master" -> "yarn", "spark.submit.deployMode" -> "client")
       val launcher = new ManagerLauncher(buildConfig(systemConfMap), baseContextConf, "", "", "", "/test/directory", stubbedSparkLauncher, environment)

       launcher.start()

       val driverOptions = stubbedSparkLauncher.getLauncherConfig().get("--driver-java-options").asInstanceOf[java.util.ArrayList[String]]
       driverOptions.get(0).trim() should be("-Xloggc:/test/directory/gc.out    -DLOG_DIR=/test/directory")
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
    final val APP_NAME = "fake.spark.app.name"
    final val APP_ARGS = "fake.spark.app.args"
    final val SPARK_ARGS = "fake.spark.conf"
    final val SPARK_DRIVER_SUPERVISE = "fake.spark.driver.supervise"
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

      override def setAppName(appName: String): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.APP_NAME, appName)
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

      override def addSparkArg(value: String): SparkLauncher = {
        launcherConfig.put(StubbedSparkLauncher.SPARK_DRIVER_SUPERVISE, value)
        null
      }

      override def startApplication(listeners: SparkAppHandle.Listener*): SparkAppHandle = {
        // Don't do anything
        null
      }
    }

