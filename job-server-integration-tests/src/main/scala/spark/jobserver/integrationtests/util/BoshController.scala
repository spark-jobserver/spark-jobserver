package spark.jobserver.integrationtests.util

import scala.sys.process._

import com.softwaremill.sttp._
import com.typesafe.config.Config
import com.typesafe.config.ConfigException

import play.api.libs.json.JsValue
import play.api.libs.json.Json

class BoshController(config: Config) extends DeploymentController(config: Config) {

  /*
   * Initialization
   */

  // Constructor
  val deployment = getDeploymentFromConfig(config)
  val instances = getInstances(deployment)

  private def getDeploymentFromConfig(config: Config): String = {
    try {
      config.getString("deployment")
    } catch {
      case e: ConfigException =>
        println("Configuration is invalid. Cannot run tests.")
        e.printStackTrace()
        sys.exit(-1)
    }
  }

  private def getInstances(deployment: String): Seq[JsValue] = {
    val cmd = s"bosh -d $deployment instances --json"
    val boshInstancesOutput = (cmd !!)
    val raw = Json.parse(boshInstancesOutput)
    (raw \\ "Rows").head.as[Seq[JsValue]] // Head because "\\" returns list with 1 element
  }

  /*
   * Helper
   */

  def getInstanceIDByIp(address: String): Option[String] = {
    val uri = uri"$address"
    val ip = uri.host
    instances.foreach { instance =>
      if ((instance \ "ips").as[String] == ip) {
        return Some((instance \ "instance").as[String])
      }
    }
    println(s"No instance with IP $ip known. These are all the instances I know:")
    println(instances)
    None
  }

  /*
   * Interface
   */

  override def stopJobserver(address: String): Boolean = {
    val uri = uri"$address"
    val ip = uri.host
    val instanceId = getInstanceIDByIp(ip)
    if (instanceId.isDefined) {
      val command = s"bosh -d $deployment stop -n ${instanceId.get}"
      (command !!)
      true
    } else {
      false
    }

  }

  override def startJobserver(address: String): Boolean = {
    val uri = uri"$address"
    val ip = uri.host
    val instanceId = getInstanceIDByIp(ip)
    if (instanceId.isDefined) {
      val command = s"bosh -d $deployment -n start ${instanceId.get}"
      (command !!)
      true
    } else {
      false
    }
  }

  override def isJobserverUp(address: String): Boolean = {
    try {
      implicit val backend = HttpURLConnectionBackend()
      val healthCheck = sttp.get(uri"$address/healthz").send()
      return healthCheck.code == 200
    } catch {
      case _: Throwable => return false
    }
  }

}