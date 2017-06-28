package spark.jobserver

import java.io.File
import scala.collection.JavaConverters._
import scala.concurrent.Await

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import com.typesafe.config._
import org.slf4j.LoggerFactory
import spark.jobserver.io._
import spark.jobserver.util.SparkJobUtils
import scala.concurrent.duration._

import spark.jobserver.services.JobServerServices
import spark.jobserver.util.ConfigValues._

object AkkaHttpJobServer {
  private val logger = LoggerFactory.getLogger(getClass)

  def start(args: Array[String], makeSystem: Config => ActorSystem) {

    logger.debug("Starting JobServer with config {}", config.getConfig("spark").root.render())

    implicit val system = makeSystem(config)
    val clazz           = Class.forName(JOB_DAO)
    val typesafeClazz   = Class.forName(CFG_CLASS)
    val ctor            = clazz.getDeclaredConstructor(typesafeClazz)

    try {
      checkContextPerJvm(config, CONTEXT_PER_JVM, clazz)
      val contextActor = if (CONTEXT_PER_JVM) {
        classOf[AkkaClusterSupervisorActor]
      } else {
        classOf[LocalContextSupervisorActor]
      }
      val datamanager = new DataFileDAO(config)
      val jobDAO      = ctor.newInstance(config).asInstanceOf[JobDAO]
      val daoActor    = system.actorOf(JobDAOActor.props(jobDAO), "dao-manager")
      val dataManager = system.actorOf(Props(classOf[DataManagerActor], datamanager), "data-manager")
      val binManager  = system.actorOf(Props(classOf[BinaryManager], daoActor), "binary-manager")
      val supervisor  = system.actorOf(Props(contextActor, daoActor), "context-supervisor")
      val jobInfo     = system.actorOf(Props(classOf[JobInfoActor], jobDAO, supervisor), "job-info")

      // Add initial job JARs, if specified in configuration.
      storeInitialBinaries(config, binManager)

      // Create initial contexts
      supervisor ! ContextSupervisor.AddContextsFromConfig
      JobServerServices(config, PORT, binManager, dataManager, supervisor, jobInfo).start()
    } catch {
      case e: Exception =>
        logger.error("Unable to start Spark JobServer: ", e)
        sys.exit(1)
    }
  }

  def main(args: Array[String]) {
    def makeSupervisorSystem(name: String)(config: Config): ActorSystem = {
      val supervisor     = ConfigValueFactory.fromIterable(List("supervisor").asJava)
      val configWithRole = config.withValue("akka.cluster.roles", supervisor)
      ActorSystem(name, configWithRole)
    }
    start(args, makeSupervisorSystem("JobServer")(_))
  }

  private def checkContextPerJvm(config: Config, enabled: Boolean, clazz: Class[_]): Unit = {
    if (enabled) {
      if (clazz.getName == "spark.jobserver.io.JobFileDAO") {
        throw new RuntimeException("JobFileDAO is not supported with context-per-jvm, use JobSqlDAO.")
      } else if (clazz.getName == "spark.jobserver.io.JobSqlDAO" && SQL_DAO_JDBC_URL.startsWith("jdbc:h2:mem")) {
        throw new RuntimeException("H2 mem backend is not support with context-per-jvm.")
      }
    }
  }

  private def parseInitialBinaryConfig(cfg: Config): Map[String, String] = {
    if (!cfg.isEmpty) {
      val initialJarsConfig = cfg.root
      logger.info("Adding initial job jars: {}", initialJarsConfig.render())
      initialJarsConfig.asScala.map {
        case (k, value) => (k, value.unwrapped.toString)
      }.toMap
    } else {
      Map()
    }
  }

  private def storeInitialBinaries(config: Config, binaryManager: ActorRef): Unit = {
    val initialBinaries = parseInitialBinaryConfig(LEGACY_JAR_PATH) ++ parseInitialBinaryConfig(BIN_PATH)
    if (initialBinaries.nonEmpty) {
      // Ensure that the jars exist
      for (binPath <- initialBinaries.values) {
        val f = new java.io.File(binPath)
        if (!f.exists) {
          val msg =
            if (f.isAbsolute) {
              s"Initial Binary File $binPath does not exist"
            } else {
              s"Initial Binary File $binPath (${f.getAbsolutePath}) does not exist"
            }

          throw new java.io.IOException(msg)
        }
      }

      val initialBinariesWithTypes = initialBinaries.mapValues {
        case s if s.endsWith(".jar") => (BinaryType.Jar, s)
        case s if s.endsWith(".egg") => (BinaryType.Egg, s)
        case other =>
          throw new Exception(
            "Only Jars (with extension .jar) and " +
              s"Python Egg packages (with extension .egg) are supported. Found $other")
      }

      val contextCreationTimeout = SparkJobUtils.getContextCreationTimeout(config)

      implicit val timeout = contextCreationTimeout seconds
      val future           = (binaryManager ? StoreLocalBinaries(initialBinariesWithTypes))(timeout)

      Await.result(future, contextCreationTimeout seconds) match {
        case InvalidBinary => sys.error("Could not store initial job binaries.")
        case BinaryStorageFailure(ex) =>
          logger.error("Failed to store initial binaries", ex)
          sys.error(s"Failed to store initial binaries: ${ex.getMessage}")
        case _ =>
      }
    }
  }
}
