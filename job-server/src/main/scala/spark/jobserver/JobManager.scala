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
 * SparkContext.  It is passed $deployMode $clusterAddr $actorName $systemConfigFile
 */
object JobManager {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  // Args: deployMode workDir clusterAddress systemConfig
  // Allow custom function to wait for termination. Useful in tests.
  def start(args: Array[String], makeSystem: Config => ActorSystem,
            waitForTermination: (ActorSystem, String) => Unit) {

    val deployMode = args(0)
    val clusterAddress = AddressFromURIString.parse(args(1))
    val managerName = args(2)
    val systemConfigFile = new File(args(3))

    if (!systemConfigFile.exists()) {
      System.err.println(s"Could not find system configuration file $systemConfigFile")
      sys.exit(1)
    }

    val defaultConfig = ConfigFactory.load()
    val systemConfig = ConfigFactory.parseFile(systemConfigFile).withFallback(defaultConfig)
    val config = if (deployMode == "cluster") {
      logger.info("Cluster mode: Removing akka.remote.netty.tcp.hostname from config!")
      logger.info("Cluster mode: Replacing spark.jobserver.sqldao.rootdir with container tmp dir.")
      val sqlDaoDir = Files.createTempDirectory("sqldao")
      val sqlDaoDirConfig = ConfigValueFactory.fromAnyRef(sqlDaoDir.toAbsolutePath.toString)
      systemConfig.withoutPath("akka.remote.netty.tcp.hostname")
                  .withValue("spark.jobserver.sqldao.rootdir", sqlDaoDirConfig)
    } else {
      systemConfig
    }

    val system = makeSystem(config.resolve())
    val clazz = Class.forName(config.getString("spark.jobserver.jobdao"))
    val ctor = clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    val jobDAO = ctor.newInstance(config).asInstanceOf[JobDAO]
    val daoActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO), "dao-manager-jobmanager")

    logger.info("Starting JobManager named " + managerName + " with config {}",
      config.getConfig("spark").root.render())

    val jobManager = system.actorOf(JobManagerActor.props(daoActor), managerName)

    //Join akka cluster
    logger.info("Joining cluster at address {}", clusterAddress)
    Cluster(system).join(clusterAddress)

    val reaper = system.actorOf(Props[ProductionReaper])
    reaper ! WatchMe(jobManager)

    waitForTermination(system, deployMode)
  }

  def main(args: Array[String]) {
    import scala.collection.JavaConverters._

    def makeManagerSystem(name: String)(config: Config): ActorSystem = {
      val configWithRole = config.withValue("akka.cluster.roles",
        ConfigValueFactory.fromIterable(List("manager").asJava))
      ActorSystem(name, configWithRole)
    }

    def waitForTermination(system: ActorSystem, deployMode: String) {
      if (deployMode == "cluster") {
        system.awaitTermination // Wait for actor system shutdown
      } else {
        system.registerOnTermination(System.exit(0)) // Kill process on actor system shutdown
      }
    }

    start(args, makeManagerSystem("JobServer"), waitForTermination)
  }
}

