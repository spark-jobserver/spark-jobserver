package spark.jobserver.integrationtests.util

/**
 * Interface to abstract the handling (shutdown & start) of a jobserver deployment.
 *
 * The id passed in the methods can be any kind of identification (e.g. IP, VM Id, ...)
 *
 */
trait DeploymentController {

  def stopJobserver(id: String) : Boolean

  def startJobserver(id: String): Boolean

  def isJobserverUp(id: String): Boolean

}