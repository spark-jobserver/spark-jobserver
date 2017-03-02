package spark.jobserver

import akka.actor.{ActorSystem, ActorRef}
import akka.actor.Props
import akka.pattern.ask
import com.typesafe.config.{ConfigValueFactory, Config, ConfigFactory}

import java.io.File
import spark.jobserver.io.{BinaryType, JobDAOActor, JobDAO, DataFileDAO}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/**
 * The Spark Job Server is a web service that allows users to submit and run Spark jobs, check status,
 * and view results.
 * It may offer other goodies in the future.
 * It only takes in one optional command line arg, a config file to override the default (and you can still
 * use -Dsetting=value to override)
 * -- Configuration --
 * {{{
 *   spark {
 *     master = "local"
 *     jobserver {
 *       port = 8090
 *     }
 *   }
 * }}}
 */
object JobServer {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  def start(args: Array[String], makeSystem: Config => ActorSystem) {
    val defaultConfig = ConfigFactory.load()
    val config = if (args.length > 0) {
      val configFile = new File(args(0))
      if (!configFile.exists()) {
        println("Could not find configuration file " + configFile)
        sys.exit(1)
      }
      ConfigFactory.parseFile(configFile).withFallback(defaultConfig).resolve()
    } else {
      defaultConfig
    }
    logger.info("Starting JobServer with config {}", config.getConfig("spark").root.render())
    logger.info("Spray config: {}", config.getConfig("spray.can.server").root.render())
    val port = config.getInt("spark.jobserver.port")

    // TODO: Hardcode for now to get going. Make it configurable later.
    val system = makeSystem(config)
    val clazz = Class.forName(config.getString("spark.jobserver.jobdao"))
    val ctor = clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    try {
      val contextPerJvm = config.getBoolean("spark.jobserver.context-per-jvm")
      // Check if we are using correct DB backend when context-per-jvm is enabled.
      // JobFileDAO and H2 mem is not supported.
      if (contextPerJvm) {
        if (clazz.getName == "spark.jobserver.io.JobFileDAO") {
          throw new RuntimeException("JobFileDAO is not supported with context-per-jvm, use JobSqlDAO.")
        } else if (clazz.getName == "spark.jobserver.io.JobSqlDAO" &&
          config.getString("spark.jobserver.sqldao.jdbc.url").startsWith("jdbc:h2:mem")) {
            throw new RuntimeException("H2 mem backend is not support with context-per-jvm.")
        }
      }
      val jobDAO = ctor.newInstance(config).asInstanceOf[JobDAO]
      val daoActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO), "dao-manager")
      val dataManager = system.actorOf(Props(classOf[DataManagerActor],
          new DataFileDAO(config)), "data-manager")
      val binManager = system.actorOf(Props(classOf[BinaryManager], daoActor), "binary-manager")
      val supervisor =
        system.actorOf(Props(if (contextPerJvm) { classOf[AkkaClusterSupervisorActor] }
                             else               { classOf[LocalContextSupervisorActor] }, daoActor),
                       "context-supervisor")
      val jobInfo = system.actorOf(Props(classOf[JobInfoActor], jobDAO, supervisor), "job-info")

      // Add initial job JARs, if specified in configuration.
      storeInitialBinaries(config, binManager)

      // Create initial contexts
      supervisor ! ContextSupervisor.AddContextsFromConfig
      new WebApi(system, config, port, binManager, dataManager, supervisor, jobInfo).start()
    } catch {
      case e: Exception =>
        logger.error("Unable to start Spark JobServer: ", e)
        sys.exit(1)
    }
  }

  private def parseInitialBinaryConfig(key: String, config: Config): Map[String, String] = {
    if (config.hasPath(key)) {
      val initialJarsConfig = config.getConfig(key).root
      logger.info("Adding initial job jars: {}", initialJarsConfig.render())
      initialJarsConfig
        .asScala
        .map { case (key, value) => (key, value.unwrapped.toString) }
        .toMap
    } else {
      Map()
    }
  }

  private def storeInitialBinaries(config: Config, binaryManager: ActorRef): Unit = {
    val legacyJarPathsKey = "spark.jobserver.job-jar-paths"
    val initialBinPathsKey = "spark.jobserver.job-bin-paths"
    val initialBinaries = parseInitialBinaryConfig(legacyJarPathsKey, config) ++
      parseInitialBinaryConfig(initialBinPathsKey, config)
    if(initialBinaries.nonEmpty) {
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
          throw new Exception(s"Only Jars (with extension .jar) and " +
            s"Python Egg packages (with extension .egg) are supported. Found $other")
      }

      val contextCreationTimeout = util.SparkJobUtils.getContextCreationTimeout(config)
      val future =
        (binaryManager ? StoreLocalBinaries(initialBinariesWithTypes))(contextCreationTimeout.seconds)

      Await.result(future, contextCreationTimeout.seconds) match {
        case InvalidBinary => sys.error("Could not store initial job binaries.")
        case BinaryStorageFailure(ex) =>
          logger.error("Failed to store initial binaries", ex)
          sys.error(s"Failed to store initial binaries: ${ex.getMessage}")
        case _ =>
      }
    }
  }

  def main(args: Array[String]) {
    import scala.collection.JavaConverters._
    def makeSupervisorSystem(name: String)(config: Config): ActorSystem = {
      val configWithRole = config.withValue("akka.cluster.roles",
        ConfigValueFactory.fromIterable(List("supervisor").asJava))
      ActorSystem(name, configWithRole)
    }
    start(args, makeSupervisorSystem("JobServer")(_))
  }


}
