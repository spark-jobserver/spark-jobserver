package spark.jobserver

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import akka.actor.{ActorSystem, Address, AddressFromURIString, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import spark.jobserver.common.akka.actor.Reaper.WatchMe
import org.slf4j.LoggerFactory
import spark.jobserver.common.akka.actor.ProductionReaper
import spark.jobserver.io.{JobDAO, JobDAOActor}
import scala.collection.JavaConverters._

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
  // Args: workDir contextContent clusterAddress
  def start(args: Array[String], makeSystem: Config => ActorSystem) {
    val clusterAddress = AddressFromURIString.parse(args(2))
    val workDir = Paths.get(args(0))
    try {
      if (!Files.exists(workDir)) {
        System.err.println(s"WorkDir $workDir does not exist, creating.")
        Files.createDirectories(workDir)
      }

      //Create context.conf in the work dir with the config string
      Files.write(workDir.resolve("context.conf"),
        Seq(args(1)).asJava,
        Charset.forName("UTF-8"))
      // Set system property LOG_DIR
      System.setProperty("LOG_DIR", args(0))
    } catch {
      case e: IOException =>
        System.err.println(s"Write context config into temp work directory $workDir, error: ${e.getMessage}")
        sys.exit(1)
    }

    val contextConfig = ConfigFactory.parseString(args(1))
    val managerName = contextConfig.getString("context.actorname")

    val defaultConfig = ConfigFactory.load()
    val config = if (args.length > 3) {
      val configFile = new File(args(3))
      if (!configFile.exists()) {
        System.err.println("Could not find configuration file " + configFile)
        sys.exit(1)
      }
      ConfigFactory.parseFile(configFile).withFallback(defaultConfig)
    } else {
      defaultConfig
    }

    val system = makeSystem(config.resolve())
    val clazz = Class.forName(config.getString("spark.jobserver.jobdao"))
    val ctor = clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    val jobDAO = ctor.newInstance(config).asInstanceOf[JobDAO]
    val daoActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO), "dao-manager-jobmanager")

    logger.info("Starting JobManager named " + managerName + " with config {}",
      config.getConfig("spark").root.render())
    logger.info("..and context config:\n" + contextConfig.root.render)

    val jobManager = system.actorOf(JobManagerActor.props(contextConfig, daoActor), managerName)

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

