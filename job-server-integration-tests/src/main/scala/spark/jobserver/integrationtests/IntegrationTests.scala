package spark.jobserver.integrationtests

import java.io.File

import org.scalatest.ConfigMap

import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions

import spark.jobserver.integrationtests.util.TestHelper

object IntegrationTests extends App {

  // Parse config
  if (args.length != 1) {
    printUsage()
    sys.exit(-1)
  }
  val file = new File(args(0))
  if (!file.exists()) {
    println(s"Could not find a config file for path ${file.getAbsolutePath}")
    sys.exit(-1)
  }
  val config = try {
    ConfigFactory.parseFile(file)
  } catch {
    case t: Throwable =>
      println("Could not parse config file: ")
      t.printStackTrace()
      sys.exit(-1)
  }

  // Validate config
  try {
    val addresses = config.getStringList("jobserverAddresses")
    if (addresses.isEmpty()) {
      println("The list of jobserverAddresses is empty. Not running any tests.")
      sys.exit(-1)
    }
    val testsToRun = config.getStringList("runTests")
    if (testsToRun.isEmpty()) {
      println("The list of tests to run is empty. Not running any tests.")
      sys.exit(-1)
    }
  } catch {
    case e: ConfigException =>
      println("Invalid configuration file: " + e.getMessage)
      sys.exit(-1)
  }

  // In case HTTPS is used, just disable verification
  if (config.hasPath("useSSL") && config.getBoolean("useSSL")) {
    TestHelper.disableSSLVerification()
  }

  // Run selected integration tests
  println("Running integration tests with the following configuration:")
  println(config.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(true)))
  val testsToRun = config.getStringList("runTests").toArray()
  testsToRun.foreach { t =>
    val testName = s"spark.jobserver.integrationtests.tests.$t"
    val clazz = Class.forName(testName)
    val test = clazz.getDeclaredConstructor()
      .newInstance().asInstanceOf[org.scalatest.Suite]
    test.execute(configMap = ConfigMap(("config", config)))
  }

  // Usage
  def printUsage() {
    println("Usage: IntegrationTests </path/to/config/file>")
  }

}
