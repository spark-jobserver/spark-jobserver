package spark.jobserver

import java.io.File

import akka.actor.{AddressFromURIString, Address, Props, ActorSystem}
import akka.cluster.Cluster
import com.typesafe.config.{ConfigValueFactory, ConfigFactory, Config}
import ooyala.common.akka.actor.ProductionReaper
import ooyala.common.akka.actor.Reaper.WatchMe
import org.slf4j.LoggerFactory
import spark.jobserver.io.{JobDAOActor, JobDAO}

object JobManager {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  def start(args: Array[String], makeSystem: Config => ActorSystem) {
    val managerName = args(0)
    val clusterAddress = AddressFromURIString.parse(args(1))

    val defaultConfig = ConfigFactory.load()
    val config = if (args.length > 2) {
      val configFile = new File(args(2))
      if (!configFile.exists()) {
        println("Could not find configuration file " + configFile)
        sys.exit(1)
      }
      ConfigFactory.parseFile(configFile).withFallback(defaultConfig)
    } else {
      defaultConfig
    }
    logger.info("Starting JobManager named " + managerName + " with config {}",
      config.getConfig("spark").root.render())

    val system = makeSystem(config)
    val jobManager = system.actorOf(Props(classOf[JobManagerActor]), managerName)

    //Join akka cluster
    logger.info("Joining cluster at address {}", clusterAddress)
    Cluster(system).join(clusterAddress)

    //Kill process on actor system shutdown
    val reaper = system.actorOf(Props[ProductionReaper])
    system.registerOnTermination(System.exit(0))
    reaper ! WatchMe(jobManager)
  }

  def main(args: Array[String]) {
    import scala.collection.JavaConverters._
    def makeManagerSystem(name: String)(config: Config): ActorSystem = {
      val configWithRole = config.withValue("akka.cluster.roles",
        ConfigValueFactory.fromIterable(List("manager").asJava))
      ActorSystem(name, configWithRole)
    }
    start(args, makeManagerSystem("JobServer")(_))
  }
}

