/**
  * This code is a modified version of parts of "Akka-cluster-custom-downing" project:
  * https://github.com/TanUkkii007/akka-cluster-custom-downing
  */

package akka.downing

import akka.ConfigurationException
import akka.actor.{ActorSystem, Props}
import akka.cluster.{Cluster, DowningProvider}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spark.jobserver.util.JobServerRoles

import scala.concurrent.duration._

class KeepOldestDowningProvider(system: ActorSystem) extends DowningProvider  {

  private[this] val cluster = Cluster(system)

  private val config: Config = system.settings.config

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Time margin after which shards or singletons that belonged to a downed/removed
    * partition are created in surviving partition. The purpose of this margin is that
    * in case of a network partition the persistent actors in the non-surviving partitions
    * must be stopped before corresponding persistent actors are started somewhere else.
    * This is useful if you implement downing strategies that handle network partitions,
    * e.g. by keeping the larger side of the partition and shutting down the smaller side.
    */
  override def downRemovalMargin: FiniteDuration = {
    val key = "custom-downing.down-removal-margin"
    if (config.hasPath(key)) {
      config.getString(key) match {
        case "off" =>
          logger.info("Setting downRemovalMargin to: 0")
          Duration.Zero
        case _ =>
          logger.info(s"Setting downRemovalMargin to: ${config.getString(key)}")
          Duration(config.getDuration(key, MILLISECONDS), MILLISECONDS)
      }
    } else {
      logger.info("DownRemovalMargin is not set in config. Defaulting to 0")
      Duration.Zero
    }
  }

  /**
    * If a props is returned it is created as a child of the core cluster daemon on cluster startup.
    * It should then handle downing using the regular [[akka.cluster.Cluster]] APIs.
    * The actor will run on the same dispatcher as the cluster actor if dispatcher not configured.
    *
    * May throw an exception which will then immediately lead to Cluster stopping, as the downing
    * provider is vital to a working cluster.
    */
  override def downingActorProps: Option[Props] = {
    val stableAfter = system.settings.config.getDuration("custom-downing.stable-after").toMillis millis

    if (stableAfter == Duration.Zero) {
      throw new ConfigurationException(
        "Downing provider will down the oldest node if it's alone," +
          "so stable-after timeout must be greater than zero.")
    }

    val preferredOldestMemberRole = {
      val r = system.settings.config.getString(
        "custom-downing.oldest-auto-downing.preferred-oldest-member-role")
      if (r.isEmpty) None else Some(r)
    }
    val calledBy = system.settings.config.getString(JobServerRoles.propertyName)
    val superviseModeOn = system.settings.config.getBoolean("spark.driver.supervise")
    val shutdownActorSystem = if (superviseModeOn) {
       if (calledBy == JobServerRoles.jobserverMaster) {
        logger.info("DowningProvider: on split brain resolution will shutdown actor system.")
        true
      } else {
        logger.info("DowningProvider: on split brain resolution will exit JMV with exit code -1.")
        false
      }
    } else {
      logger.info("DowningProvider: on split brain resolution will" +
        "shutdown actor system (as supervise mode is off).")
      true
    }

    Some(KeepOldestAutoDown.props(preferredOldestMemberRole, shutdownActorSystem, stableAfter))
  }
}
