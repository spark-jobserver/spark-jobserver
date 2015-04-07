package spark.jobserver

import akka.actor.ActorRef
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.io.{JobDAOActor, JobDAO}
import spark.jobserver.util.JarUtils
import org.joda.time.DateTime

// Messages to JarManager actor
case class StoreJar(appName: String, jarBytes: Array[Byte])
case object ListJars

// Responses
case object InvalidJar
case object JarStored

/**
 * An Actor that manages the jars stored by the job server.   It's important that threads do not try to
 * load a class from a jar as a new one is replacing it, so using an actor to serialize requests is perfect.
 */
class JarManager(jobDao: ActorRef) extends InstrumentedActor {

  import scala.concurrent.duration._
  val daoAskTimeout = Timeout(3 seconds)

  override def wrappedReceive: Receive = {
    case ListJars =>
      import akka.pattern.{ask, pipe}
      import context.dispatcher

      val resp = (jobDao ? JobDAOActor.GetApps)(daoAskTimeout).mapTo[JobDAOActor.Apps]
      resp.map { msg => msg.apps } pipeTo sender()


    case StoreJar(appName, jarBytes) =>
      logger.info("Storing jar for app {}, {} bytes", appName, jarBytes.size)
      if (!JarUtils.validateJarBytes(jarBytes)) {
        sender ! InvalidJar
      } else {
        val uploadTime = DateTime.now()
        jobDao ! JobDAOActor.SaveJar(appName, uploadTime, jarBytes)
        sender ! JarStored
      }
  }

}
