package spark.jobserver

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.util.Try

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent}
import com.typesafe.config.{Config, ConfigRenderOptions}
import spark.jobserver.util.SparkJobUtils
import scala.sys.process._

/**
 * The AkkaClusterSupervisorActor launches Spark Contexts as external processes
 * that connect back with the master node via Akka Cluster.
 *
 * Currently, when the Supervisor gets a MemberUp message from another actor,
 * it is assumed to be one starting up, and it will be asked to identify itself,
 * and then the Supervisor will try to initialize it.
 *
 * See the [[LocalContextSupervisorActor]] for normal config options.  Here are ones
 * specific to this class.
 *
 * ==Configuration==
 * {{{
 *   deploy {
 *     manager-start-cmd = "./manager_start.sh"
 *   }
 * }}}
 */
class AkkaClusterSupervisorActor(daoActor: ActorRef) extends BaseSupervisorActor {
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val contextInitTimeout = config.getDuration("spark.context-settings.context-init-timeout",
                                                TimeUnit.SECONDS)
  val managerStartCommand = config.getString("deploy.manager-start-cmd")
  import context.dispatcher

  //actor name -> (context isadhoc, success callback, failure callback)
  //TODO: try to pass this state to the jobManager at start instead of having to track
  //extra state.  What happens if the WebApi process dies before the forked process
  //starts up?  Then it never gets initialized, and this state disappears.
  protected val contextInitInfos =
    mutable.HashMap.empty[String, (Boolean, ActorRef => Unit, Throwable => Unit)]

  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress

  logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)

  override def preStart(): Unit = {
    cluster.join(selfAddress)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    cluster.leave(selfAddress)
  }

  protected def onMemberUp(member: Member): Unit = {
    if (member.hasRole("manager")) {
      val memberActors = RootActorPath(member.address) / "user" / "*"
      context.actorSelection(memberActors) ! Identify(memberActors)
    }
  }

  protected def onActorIdentity(memberActors: Any, actorRefOpt: Option[ActorRef]): Unit = {
    actorRefOpt.foreach { (actorRef) =>
      val actorName = actorRef.path.name
      if (actorName.startsWith("jobManager")) {
        logger.info("Received identify response, attempting to initialize context at {}", memberActors)
        (for { (isAdHoc, successFunc, failureFunc) <- contextInitInfos.remove(actorName) }
         yield {
           initContext(actorName, actorRef, getTimeout())(isAdHoc, successFunc, failureFunc)
         }).getOrElse({
          logger.warn("No initialization or callback found for jobManager actor {}", actorRef.path)
          actorRef ! PoisonPill
        })
      }
    }
  }

  protected def onStopContext(actor: ActorRef): Unit = {
    cluster.down(actor.path.address)
  }
  protected def getTimeout(): Long = contextInitTimeout

  protected def startContext(
    name: String, actorName: String, contextConfig: Config, isAdHoc: Boolean
  )(successFunc: ActorRef => Unit, failureFunc: Throwable => Unit): Unit = {

    logger.info("Starting context with actor name {}", actorName)

    val contextDir: java.io.File = try {
      createContextDir(name, contextConfig, isAdHoc, actorName)
    } catch {
      case e: Exception =>
        failureFunc(e)
        return
    }

    //extract spark.proxy.user from contextConfig, if available and pass it to $managerStartCommand
    var cmdString = s"$managerStartCommand $contextDir ${selfAddress.toString}"

    if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
      cmdString = cmdString + s" ${contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM)}"
    }

    val pb = Process(cmdString)
    val pio = new ProcessIO(
      (_) => (),
      (stdout) => scala.io.Source.fromInputStream(stdout)
        .getLines.foreach(println),
      (stderr) => scala.io.Source.fromInputStream(stderr).getLines().foreach(println)
    )
    logger.info("Starting to execute sub process {}", pb)
    val processStart = Try {
      val process = pb.run(pio)
      val exitVal = process.exitValue()
      if (exitVal != 0) {
        throw new IOException("Failed to launch context process, got exit code " + exitVal)
      }
    }

    if (processStart.isSuccess) {
      contextInitInfos(actorName) = (isAdHoc, successFunc, failureFunc)
    } else {
      failureFunc(processStart.failed.get)
    }
  }

  private def createContextDir(
    name: String,
    contextConfig: Config,
    isAdHoc: Boolean,
    actorName: String
   ): java.io.File = {
    // Create a temporary dir, preferably in the LOG_DIR
    val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")

    val tmpDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$encodedContextName")
    }.getOrElse(Files.createTempDirectory("jobserver"))

    logger.info("Created working directory {} for context {}", tmpDir: Any, name)

    // Now create the contextConfig merged with the values we need
    val mergedConfig = createMergedActorConfig(name, actorName, contextConfig, isAdHoc)

    // Write out the config to the temp dir
    Files.write(
      tmpDir.resolve("context.conf"),
      Seq(mergedConfig.root.render(ConfigRenderOptions.concise)).asJava,
      Charset.forName("UTF-8")
    )

    tmpDir.toFile
  }

  // we don't need any sleep between contexts in a cluster
  override protected def createContextSleep(): Unit = {
  }


}
