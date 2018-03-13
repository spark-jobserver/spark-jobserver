package spark.jobserver

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Address, AddressFromURIString, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import spark.jobserver.common.akka.actor.Reaper.WatchMe
import org.slf4j.LoggerFactory
import spark.jobserver.common.akka.actor.ProductionReaper
import spark.jobserver.io.{JobDAO, JobDAOActor}
import scala.collection.JavaConverters._
import scala.util.Try
import scala.concurrent.duration.FiniteDuration

/**
 * The JobManager is the main entry point for the forked JVM process running an individual
 * SparkContext.  It is passed $clusterAddr $actorName $systemConfigFile
 */
object JobManager {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  // Args: workDir clusterAddress systemConfig
  // Allow custom function to wait for termination. Useful in tests.
  def start(args: Array[String], makeSystem: Config => ActorSystem,
            waitForTermination: (ActorSystem, String, String) => Unit) {

    val clusterAddress = AddressFromURIString.parse(args(0))
    val managerName = args(1)
    val systemConfigFile = new File(args(2))

    if (!systemConfigFile.exists()) {
      System.err.println(s"Could not find system configuration file $systemConfigFile")
      sys.exit(1)
    }

    val defaultConfig = ConfigFactory.load()
    val systemConfig = ConfigFactory.parseFile(systemConfigFile).withFallback(defaultConfig)
    val master = Try(systemConfig.getString("spark.master")).toOption
      .getOrElse("local[4]").toLowerCase()
    val deployMode = Try(systemConfig.getString("spark.submit.deployMode")).toOption
      .getOrElse("client").toLowerCase()
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

    val masterAddress = systemConfig.getBoolean("spark.jobserver.kill-context-on-supervisor-down") match {
      case true => clusterAddress.toString + "/user/context-supervisor"
      case false => ""
    }

    val contextId = managerName.replace(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX, "")
    val jobManager = system.actorOf(JobManagerActor.props(daoActor, masterAddress, contextId,
        getManagerInitializationTimeout(systemConfig)), managerName)

    //Join akka cluster
    logger.info("Joining cluster at address {}", clusterAddress)
    Cluster(system).join(clusterAddress)

    val reaper = system.actorOf(Props[ProductionReaper])
    reaper ! WatchMe(jobManager)

    waitForTermination(system, master, deployMode)
  }

  def main(args: Array[String]) {
    import scala.collection.JavaConverters._

    def makeManagerSystem(name: String)(config: Config): ActorSystem = {
      val configWithRole = config.withValue("akka.cluster.roles",
        ConfigValueFactory.fromIterable(List("manager").asJava))
      ActorSystem(name, configWithRole)
    }

    def waitForTermination(system: ActorSystem, master: String, deployMode: String) {
      if (master == "yarn" && deployMode == "cluster") {
        // YARN Cluster Mode:
        // Finishing the main method means that the job has been done and immediately finishes
        // the driver process, that why we have to wait here.
        // Calling System.exit results in a failed YARN application result:
        // org.apache.spark.deploy.yarn.ApplicationMaster#runImpl() in Spark
        system.awaitTermination
      } else {
        // Spark Standalone Cluster Mode:
        // We have to call System.exit(0) otherwise the driver process keeps running
        // after the context has been stopped.
        system.registerOnTermination(System.exit(0))
      }
    }

    start(args, makeManagerSystem("JobServer"), waitForTermination)
  }

   private def getManagerInitializationTimeout(config: Config): FiniteDuration = {
    FiniteDuration(config.getDuration("spark.jobserver.manager-initialization-timeout",
        TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }
}

