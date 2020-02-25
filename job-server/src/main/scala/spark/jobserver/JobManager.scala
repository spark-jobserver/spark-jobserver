package spark.jobserver

import java.io.InputStreamReader
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, AddressFromURIString, Props}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer.InvalidConfiguration
import spark.jobserver.common.akka.actor.ProductionReaper
import spark.jobserver.common.akka.actor.Reaper.WatchMe
import spark.jobserver.io.JobDAOActor.GetContextInfo
import spark.jobserver.util.{JobServerRoles, JobserverConfig}
import spark.jobserver.io.{BinaryDAO, ContextStatus, JobDAOActor, MetaDataDAO}
import spark.jobserver.util.{HadoopFSFacade, NetworkAddressFactory, Utils}

import scala.concurrent.Await
import scala.concurrent.duration._
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
            waitForTermination: (ActorSystem, String, String, ActorRef, String) => Unit) {

    val masterAddresses = args(0)
    val masterSeedNodes = Try {
      masterAddresses
        .split(',')
        .map(AddressFromURIString.parse)
        .toList
      }.getOrElse {
        logger.info(s"Failed to parse master seed node address(es) ${masterAddresses} in JVM arguments.")
        exitJVM
      }
    logger.info(s"Found master seed nodes are: ${masterAddresses}")

    val managerName = args(1)
    val loadedConfig = getConfFromFS(args(2)).getOrElse(exitJVM).withValue(
      JobServerRoles.propertyName, ConfigValueFactory.fromAnyRef(JobServerRoles.jobserverSlave))
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

    val metadataDaoClass = Class.forName(config.getString(JobserverConfig.METADATA_DAO_CONFIG_PATH))
    val binaryDaoClass = Class.forName(config.getString(JobserverConfig.BINARY_DAO_CONFIG_PATH))
    val ctorMeta = metadataDaoClass.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    val ctorBin = binaryDaoClass.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    val metaDataDAO = try {
      ctorMeta.newInstance(config).asInstanceOf[MetaDataDAO]
    } catch {
      case _: ClassNotFoundException =>
        throw new InvalidConfiguration("Couldn't create Metadata DAO instance: please check configuration")
    }
    val binaryDAO = try {
      ctorBin.newInstance(config).asInstanceOf[BinaryDAO]
    } catch {
      case _: ClassNotFoundException =>
        throw new InvalidConfiguration("Couldn't create Binary DAO instance: please check configuration")
    }
    val daoActor = system.actorOf(
      Props(classOf[JobDAOActor], metaDataDAO, binaryDAO, config), "dao-manager-jobmanager")

    logger.info("Starting JobManager named " + managerName + " with config {}",
      config.getConfig("spark").root.render())

    val masterAddress = systemConfig.getBoolean("spark.jobserver.kill-context-on-supervisor-down") match {
      /*
       * TODO
       * Note: This zombie killing logic has to be replaced by a proper split brain
       * resolver, since the fix of resolving the AkkaClusterSupervisor no longer works
       * when there is more than one Jobserver in the cluster (as it might be there or not).
       */
      case true => masterSeedNodes.head.toString + "/user/singleton/context-supervisor"
      case false => ""
    }

    val contextId = managerName.replace(JobserverConfig.MANAGER_ACTOR_PREFIX, "")
    val jobManager = system.actorOf(JobManagerActor.props(daoActor, masterAddress, contextId,
        getManagerInitializationTimeout(systemConfig)), managerName)

    // Join akka cluster
    logger.info("Joining cluster at address(es) {}", masterSeedNodes.mkString(", "))
    Cluster(system).joinSeedNodes(masterSeedNodes)

    val reaper = system.actorOf(Props[ProductionReaper])
    reaper ! WatchMe(jobManager)

    waitForTermination(system, master, deployMode, daoActor, contextId)
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
  private def exitJVM = {
    logger.warn("Exiting the JVM with status code of 0")
    sys.exit(0)
  }

  def main(args: Array[String]) {
    import scala.collection.JavaConverters._

    def makeManagerSystem(name: String)(config: Config): ActorSystem = {
      val configWithRole = config.withValue("akka.cluster.roles",
        ConfigValueFactory.fromIterable(List("manager").asJava))
      ActorSystem(name, configWithRole)
    }

    def waitForTermination(system: ActorSystem, master: String,
                           deployMode: String, daoActor: ActorRef,
                           contextId: String) {
      if (master == "yarn" && deployMode == "cluster") {
        // YARN Cluster Mode:
        // Finishing the main method means that the job has been done and immediately finishes
        // the driver process, that why we have to wait here.
        // Calling System.exit results in a failed YARN application result:
        // org.apache.spark.deploy.yarn.ApplicationMaster#runImpl() in Spark
        Await.result(system.terminate(), Duration.Inf)
      } else {
        // Spark Standalone Cluster Mode:
        // We have to call System.exit(0) otherwise the driver process keeps running
        // after the context has been stopped.
        system.registerOnTermination {
          logger.info("Actor system terminated. Exiting the JVM with exit code 0.")
          System.exit(0)
        }

        // We have to call Runtime.getRuntime.halt(-1) otherwise the driver process may be stopped
        // although it was just a network glitch. MemberRemoved event should happen even if
        // driver exits the cluster by itself. Exiting with -1 is okay as Jobserver won't allow
        // stopped context to join again.
        // Increase system termination timeout so that system has time to shutdown by
        // itself (in case of a planned shutdown).
        Cluster(system).registerOnMemberRemoved {
          logger.info("JobManagerActor was removed from the Akka cluster.")
          // We must spawn a separate thread to not block current thread,
          // since that would have blocked the shutdown of the ActorSystem.
          new Thread {
            override def run(): Unit = {
              val futureTimeout = 180.seconds
              implicit val akkaTimeout = Timeout(futureTimeout)
              if (Try(Await.ready(system.whenTerminated, futureTimeout)).isFailure) {
                Try(Await.result(daoActor ? GetContextInfo(contextId), futureTimeout)) match {
                  case Success(response) =>
                    val contextInfo = response.asInstanceOf[JobDAOActor.ContextResponse].contextInfo
                    contextInfo match {
                      case Some(info) if ContextStatus.getNonFinalStates().contains(info.state) =>
                        logger.info(s"JobManagerActor has status ${info.state}, but was kicked out of " +
                          s"the cluster minimum 3 minutes ago. Exiting JVM with -1.")
                        Runtime.getRuntime.halt(-1)
                      case Some(info) =>
                        logger.info(s"Context ${info.name} is in state ${info.state}. Akka hook for removal" +
                          s" from the cluster won't take any action in this case.")
                      case None =>
                        logger.info(s"Akka hook for removal from the cluster couldn't obtain context " +
                          s"information for context id $contextId")
                    }
                  case Failure(_) =>
                    logger.info(s"Akka hook for removal from the cluster couldn't get information from" +
                      s" DAO actor. Taking no action.")
                }
              }
            }
          }.start()
        }
      }
    }

    start(args, makeManagerSystem("JobServer"), waitForTermination)
  }

  private def getManagerInitializationTimeout(config: Config): FiniteDuration = {
    FiniteDuration(config.getDuration("spark.jobserver.manager-initialization-timeout",
        TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }
}
