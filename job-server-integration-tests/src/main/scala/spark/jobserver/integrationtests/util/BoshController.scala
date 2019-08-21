package spark.jobserver.integrationtests.util

import sys.process._
import com.softwaremill.sttp._
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import com.typesafe.config.ConfigFactory
import java.io.File

class BoshController extends DeploymentController{

  /*
   * Initialization
   */

  // Constructor
  val deployment = getDeploymentFromConfig("bosh.conf")
  val instances = getInstances(deployment)

  private def getDeploymentFromConfig(configFile: String) : String = {
    val file = new File(configFile)
    val config = ConfigFactory.parseFile(file)
    config.getString("deployment")
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

  def getInstanceIDByIp(ip: String): Option[String] = {
    instances.foreach { instance =>
      if((instance \ "ips").as[String] == ip){
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

  override def stopJobserver(ip: String) : Boolean = {
    val instanceId = getInstanceIDByIp(ip)
    if(instanceId.isDefined){
      val command = s"bosh -d $deployment stop -n ${instanceId.get}"
      (command !!)
      true
    } else {
      false
    }

  }

  override def startJobserver(ip: String): Boolean = {
    val instanceId = getInstanceIDByIp(ip)
    if(instanceId.isDefined){
      val command = s"bosh -d $deployment -n start ${instanceId.get}"
      (command !!)
      true
    } else {
      false
    }
  }

  override def isJobserverUp(ip: String): Boolean = {
    try{
    implicit val backend = HttpURLConnectionBackend()
    val healthCheck = sttp.get(uri"$ip:8090/healthz").send()
    return healthCheck.code == 200
    } catch {
      case _ : Throwable => return false
    }
  }

}