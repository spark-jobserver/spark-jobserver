package spark.jobserver

import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.{ConfigValueFactory, Config, ConfigFactory}
import java.io.File
import spark.jobserver.io.{JobDAOActor, JobDAO}
import org.slf4j.LoggerFactory

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
      ConfigFactory.parseFile(configFile).withFallback(defaultConfig)
    } else {
      defaultConfig
    }
    logger.info("Starting JobServer with config {}", config.getConfig("spark").root.render())
    val port = config.getInt("spark.jobserver.port")

    // TODO: Hardcode for now to get going. Make it configurable later.
    val system = makeSystem(config)
    val clazz = Class.forName(config.getString("spark.jobserver.jobdao"))
    val ctor = clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    val jobDAO = ctor.newInstance(config).asInstanceOf[JobDAO]
    val daoActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO), "dao-manager")

    val jarManager = system.actorOf(Props(classOf[JarManager], daoActor), "jar-manager")
    val supervisor = system.actorOf(Props(classOf[AkkaClusterSupervisorActor], daoActor),
      "context-supervisor")
    val jobInfo = system.actorOf(Props(classOf[JobInfoActor], daoActor, supervisor), "job-info")

    // Create initial contexts
    supervisor ! ContextSupervisor.AddContextsFromConfig
    new WebApi(system, config, port, jarManager, supervisor, jobInfo).start()

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
