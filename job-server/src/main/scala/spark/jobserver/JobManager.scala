package spark.jobserver

import java.io.InputStreamReader
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, AddressFromURIString, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import spark.jobserver.common.akka.actor.ProductionReaper
import spark.jobserver.common.akka.actor.Reaper.WatchMe
import spark.jobserver.io.{JobDAO, JobDAOActor}
import spark.jobserver.util.{HadoopFSFacade, NetworkAddressFactory, Utils}

import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

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
    val loadedConfig = getConfFromFS(args(2)).getOrElse(exitJVM)
    val defaultConfig = ConfigFactory.load()
    var systemConfig = loadedConfig.withFallback(defaultConfig)
    val master = Try(systemConfig.getString("spark.master")).toOption
      .getOrElse("local[4]").toLowerCase()
    val deployMode = Try(systemConfig.getString("spark.submit.deployMode")).toOption
      .getOrElse("client").toLowerCase()

    val config = if (deployMode == "cluster") {
      Try(getNetworkAddress(systemConfig)) match {
        case Success(Some(address)) =>
          logger.info(s"Cluster mode: Setting akka.remote.netty.tcp.hostname to ${address}!")
          systemConfig = systemConfig.withValue("akka.remote.netty.tcp.hostname",
            ConfigValueFactory.fromAnyRef(address))
        case Success(None) => // Don't change hostname
        case Failure(e) =>
          logger.error("Exception during network address resolution", e)
          exitJVM
      }

      logger.info("Cluster mode: Replacing spark.jobserver.sqldao.rootdir with container tmp dir.")
      val sqlDaoDir = Files.createTempDirectory("sqldao")
      FileUtils.forceDeleteOnExit(sqlDaoDir.toFile)
      val sqlDaoDirConfig = ConfigValueFactory.fromAnyRef(sqlDaoDir.toAbsolutePath.toString)
      systemConfig.withValue("spark.jobserver.sqldao.rootdir", sqlDaoDirConfig)
                  .withoutPath("akka.remote.netty.tcp.port")
                  .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0))
    } else {
      systemConfig
    }

    val system = makeSystem(config.resolve())
    val clazz = Class.forName(config.getString("spark.jobserver.jobdao"))
    val ctor = clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    val jobDAO = ctor.newInstance(config).asInstanceOf[JobDAO]

    val migrationActor = try {
      val duration = FiniteDuration(3, SECONDS)
      Await.result(system.actorSelection(
        clusterAddress.toString + "/user/migration-actor"
      ).resolveOne(duration), duration)
    } catch {
      // Failing to find migration worker shouldn't fail JobManager creation
      case NonFatal(e) =>
        logger.error(s"Failed to obtain ActorRef for Zookeeper migration actor: ${e.getMessage}")
        logger.error(s"Creating own instance of Migration Actor:")
        system.actorOf(MigrationActor.props(config), s"$managerName-migration-actor")
    }

    val daoActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO,
      migrationActor, clusterAddress.toString + "/user/migration-actor"), "dao-manager-jobmanager")

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

  private def getConfFromFS(path: String): Option[Config] = {
    new HadoopFSFacade(defaultFS = "file:///").get(path) match {
      case Some(stream) =>
        // Since config contains characters, we convert the input stream
        // to InputStreamReader.
        val reader = new InputStreamReader(stream)
        Try(Utils.usingResource(reader)(ConfigFactory.parseReader)) match {
          case Success(config) => Some(config)
          case Failure(t) =>
            logger.error(t.getMessage)
            None
        }
      case None => None
    }
  }

  private def getNetworkAddress(systemConfig: Config): Option[String] = {
    systemConfig.hasPath("spark.jobserver.network-address-resolver") match {
      case true =>
        val strategyShortName = systemConfig.getString("spark.jobserver.network-address-resolver")
        NetworkAddressFactory(strategyShortName).getAddress()
      case false => None
    }
  }

  /**
    * 0 is used as exit code to avoid restart of JVM by Spark in supervise mode
    */
  private def exitJVM = sys.exit(0)

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

