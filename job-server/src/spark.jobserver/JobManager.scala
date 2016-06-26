package spark.jobserver

import akka.actor.{ActorSystem, AddressFromURIString, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import ooyala.common.akka.actor.ProductionReaper
import ooyala.common.akka.actor.Reaper.WatchMe
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asJavaIterable

/**
 * The JobManager is the main entry point for the forked JVM process running an individual
 * SparkContext.  It is passed $workDir $clusterAddr $configFile
 *
 * Each forked process has a working directory with log files for that context only, plus
 * a file "context.conf" which contains context-specific settings.
 */
object JobManager {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  def start(args: Array[String], makeSystem: Config => ActorSystem) {
    val clusterAddress = AddressFromURIString.parse(args(0))

    val config = {
      val properties = new java.util.HashMap[String, String]()
      args(1).split('|').foreach(_.split("=", 2) match {
        case Array(key, value) => properties.put(key, value)
      })

      ConfigFactory.parseMap(properties)
        .withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromIterable(List(args(0))))
    }

    val managerName = config.getString("context.actorname")

    logger.info("Starting JobManager named " + managerName + " with config {}",
      config.getConfig("spark").root.render())
    logger.info("..and context config:\n" + config.root.render)

    val system = makeSystem(config.resolve())
    val jobManager = system.actorOf(JobManagerActor.props(config), managerName)

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

