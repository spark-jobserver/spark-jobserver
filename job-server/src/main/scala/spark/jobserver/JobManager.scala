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
 * SparkContext.  It is passed $deployMode $workDir $clusterAddr $systemConfigFile
 *
 * Each forked process has a working directory with log files for that context only, plus
 * a file "context.conf" which contains context-specific settings.
 */
object JobManager {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  // Args: deployMode workDir clusterAddress systemConfig
  def start(args: Array[String], makeSystem: Config => ActorSystem) {
    val deployMode = args(0)
    val clusterAddress = AddressFromURIString.parse(args(2))

    val (systemConfigFile, contextConfigFile) = if (deployMode == "cluster") {
      // read files from current dir
      (new File(new File(args(3)).getName), new File("context.conf"))

    } else { // client
      var workDir = Paths.get(args(1))
      System.setProperty("LOG_DIR", workDir.toAbsolutePath.toString) // export LOG_DIR
      (new File(args(3)), new File(workDir + "/context.conf"))
    }

    if (!systemConfigFile.exists()) {
      System.err.println(s"Could not find system configuration file $systemConfigFile")
      sys.exit(1)
    }

    if (!contextConfigFile.exists()) {
      System.err.println(s"Could not find context configuration file $contextConfigFile")
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
    val contextConfig = ConfigFactory.parseFile(contextConfigFile)

    val managerName = contextConfig.getString("context.actorname")
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

    val reaper = system.actorOf(Props[ProductionReaper])
    reaper ! WatchMe(jobManager)

    if (deployMode == "cluster") {
      // Wait for actor system shutdown
      system.awaitTermination
    } else {
      // Kill process on actor system shutdown
      system.registerOnTermination(System.exit(0))
    }
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

