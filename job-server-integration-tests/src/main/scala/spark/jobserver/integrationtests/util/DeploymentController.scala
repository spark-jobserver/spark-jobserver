package spark.jobserver.integrationtests.util

import com.typesafe.config.Config

/**
 * Interface to abstract the handling (shutdown & start) of a jobserver deployment.
 *
 * The id passed in the methods can be any kind of identification (e.g. IP, VM Id, ...)
 *
 */
abstract class DeploymentController(config: Config) {

  def stopJobserver(id: String): Boolean

  def startJobserver(id: String): Boolean

  def isJobserverUp(id: String): Boolean

}

object DeploymentController {
  /**
   * Load a concrete implementation of a DeploymentController which is specified in the config file.
   */
  def fromConfig(config: Config): DeploymentController = {
    val packageName = this.getClass.getPackage.getName
    try {
      val className = config.getString("deploymentController")
      val clazz = Class.forName(s"$packageName.$className")
      clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
        .newInstance(config).asInstanceOf[DeploymentController]
    } catch {
      case t: Throwable =>
        println(s"DeploymentController specified in config is invalid:")
        t.printStackTrace()
        println("Aborting tests.")
        sys.exit(-1)
    }
  }
}