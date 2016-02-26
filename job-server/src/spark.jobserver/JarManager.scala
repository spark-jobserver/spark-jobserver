package spark.jobserver

import akka.actor.ActorRef
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.io.{JobDAOActor, JobDAO}
import spark.jobserver.util.JarUtils
import org.joda.time.DateTime

import java.nio.file.{Files, Paths}

// Messages to JarManager actor

/** Message for storing a JAR for an application given the byte array of the JAR file */
case class StoreJar(appName: String, jarBytes: Array[Byte])

/** Message requesting a listing of the available JARs */
case object ListJars

/** Message for storing one or more local JARs based on the given map.
  * @param  localJars    Map where the key is the appName and the value is the local path to the JAR.
  */
case class StoreLocalJars(localJars: Map[String, String])

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

  private def saveJar(appName: String, jarBytes: Array[Byte]): Unit = {
    val uploadTime = DateTime.now()
    jobDao ! JobDAOActor.SaveJar(appName, uploadTime, jarBytes)
  }

  override def wrappedReceive: Receive = {
    case ListJars =>
      import akka.pattern.{ask, pipe}
      import context.dispatcher

      val requestor = sender
      val resp = (jobDao ? JobDAOActor.GetApps)(daoAskTimeout).mapTo[JobDAOActor.Apps]
      resp.map { msg => msg.apps } pipeTo requestor


    case StoreLocalJars(localJars) =>
      val success =
        localJars.foldLeft(true) { (success, pair) =>
          success && {
            val (appName, jarPath) = pair
            try {
              val jarBytes = Files.readAllBytes(Paths.get(jarPath))
              logger.info("Storing jar for app {}, {} bytes", appName, jarBytes.size)
              JarUtils.validateJarBytes(jarBytes) && {
                saveJar(appName, jarBytes)
                true
              }
            } catch {
              case e: Exception =>
                  logger.error(e.getMessage)
                  false
            }
          }
        }

      sender ! (if (success) { JarStored } else { InvalidJar })

    case StoreJar(appName, jarBytes) =>
      logger.info("Storing jar for app {}, {} bytes", appName, jarBytes.size)
      if (!JarUtils.validateJarBytes(jarBytes)) {
        sender ! InvalidJar
      } else {
        saveJar(appName, jarBytes)
        sender ! JarStored
      }
  }

}
