package spark.jobserver

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.util.SparkJobUtils
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.sys.process._

import spark.jobserver.common.akka.InstrumentedActor
import scala.concurrent.Await
import akka.pattern.gracefulStop

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
 *     wait-for-manager-start = true
 *   }
 * }}}
 */
class AkkaClusterSupervisorActor(daoActor: ActorRef) extends InstrumentedActor {
  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextInitTimeout = config.getDuration("spark.context-settings.context-init-timeout",
                                                TimeUnit.SECONDS)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  val managerStartCommand = config.getString("deploy.manager-start-cmd")
  val waitForManagerStart = config.getBoolean("deploy.wait-for-manager-start")
  import context.dispatcher

  //actor name -> (context isadhoc, success callback, failure callback)
  //TODO: try to pass this state to the jobManager at start instead of having to track
  //extra state.  What happens if the WebApi process dies before the forked process
  //starts up?  Then it never gets initialized, and this state disappears.
  private val contextInitInfos = mutable.HashMap.empty[String, (Boolean, ActorRef => Unit, Throwable => Unit)]

  // actor name -> (JobManagerActor ref, ResultActor ref)
  private val contexts = mutable.HashMap.empty[String, (ActorRef, ActorRef)]

  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)

  override def preStart(): Unit = {
    cluster.join(selfAddress)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    cluster.leave(selfAddress)
  }

  def wrappedReceive: Receive = {
    case MemberUp(member) =>
      if (member.hasRole("manager")) {
        val memberActors = RootActorPath(member.address) / "user" / "*"
        context.actorSelection(memberActors) ! Identify(memberActors)
      }

    case ActorIdentity(memberActors, actorRefOpt) =>
      actorRefOpt.foreach{ actorRef =>
        val actorName = actorRef.path.name
        if (actorName.startsWith("jobManager")) {
          logger.info("Received identify response, attempting to initialize context at {}", memberActors)
          (for { (isAdHoc, successFunc, failureFunc) <- contextInitInfos.remove(actorName) }
           yield {
             initContext(actorName, actorRef, contextInitTimeout)(isAdHoc, successFunc, failureFunc)
           }).getOrElse({
            logger.warn("No initialization or callback found for jobManager actor {}", actorRef.path)
            actorRef ! PoisonPill
          })
        }
      }


    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      sender ! contexts.keys.toSeq

    case AddContext(name, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      // TODO(velvia): This check is not atomic because contexts is only populated
      // after SparkContext successfully created!  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349
      if (contexts contains name) {
        originator ! ContextAlreadyExists
      } else {
        startContext(name, mergedConfig, false) { ref =>
          originator ! ContextInitialized
        } { err =>
          originator ! ContextInitError(err)
        }
      }

    case StartAdHocContext(classPath, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)

      var contextName = ""
      do {
        contextName = java.util.UUID.randomUUID().toString().take(8) + "-" + classPath
      } while (contexts contains contextName)
      // TODO(velvia): Make the check above atomic.  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349

      startContext(contextName, mergedConfig, true) { ref =>
        originator ! contexts(contextName)
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! contexts.get(name).map(_._2).getOrElse(globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        sender ! contexts(name)
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info("Shutting down context {}", name)
        val contextActorRef = contexts(name)._1
        cluster.down(contextActorRef.path.address)
        try {
          val stoppedCtx = gracefulStop(contexts(name)._1, contextDeletionTimeout seconds)
          Await.result(stoppedCtx, contextDeletionTimeout + 1 seconds)
          sender ! ContextStopped
        }
        catch {
          case err: Exception => sender ! ContextStopError(err)
        }
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      logger.info("Actor terminated: {}", name)
      contexts.retain { case (name, (jobMgr, resActor)) => jobMgr != actorRef }
  }

  private def initContext(actorName: String, ref: ActorRef, timeoutSecs: Long = 1)
                         (isAdHoc: Boolean,
                          successFunc: ActorRef => Unit,
                          failureFunc: Throwable => Unit): Unit = {
    import akka.pattern.ask

    val resultActor = if (isAdHoc) globalResultActor else context.actorOf(Props(classOf[JobResultActor]))
    (ref ? JobManagerActor.Initialize(
      Some(resultActor)))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e:Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        ref ! PoisonPill
        failureFunc(e)
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        ref ! PoisonPill
        failureFunc(t)
      case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        contexts(ctxName) = (ref, resActor)
        context.watch(ref)
        successFunc(ref)
      case _ => logger.info("Failed for unknown reason.")
        ref ! PoisonPill
        failureFunc(new RuntimeException("Failed for unknown reason."))
    }
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean)
                          (successFunc: ActorRef => Unit)(failureFunc: Throwable => Unit): Unit = {
    require(!(contexts contains name), "There is already a context named " + name)
    val contextActorName = "jobManager-" + java.util.UUID.randomUUID().toString.substring(16)

    logger.info("Starting context with actor name {}", contextActorName)

    val driverMode = Try(config.getString("spark.jobserver.driver-mode")).toOption.getOrElse("client")
    val (workDir, contextContent) = generateContext(name, contextConfig, isAdHoc, contextActorName)
    logger.info("Ready to create working directory {} for context {}", workDir: Any, name)

    //extract spark.proxy.user from contextConfig, if available and pass it to $managerStartCommand
    var cmdString = if (driverMode == "mesos-cluster") {
      s"$managerStartCommand $driverMode $workDir '$contextContent' ${selfAddress.toString}"
    } else {
      s"$managerStartCommand $driverMode $workDir $contextContent ${selfAddress.toString}"
    }

    if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
      cmdString = cmdString + s" ${contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM)}"
    }

    val pb = Process(cmdString)
    val pio = new ProcessIO(_ => (),
                        stdout => scala.io.Source.fromInputStream(stdout)
                          .getLines.foreach(println),
                        stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(println))
    logger.info("Starting to execute sub process {}", pb)
    val processStart = Try {
      val process = pb.run(pio)
      if (waitForManagerStart) {
        val exitVal = process.exitValue()
        if (exitVal != 0) {
          throw new IOException("Failed to launch context process, got exit code " + exitVal)
        }
      }
    }

    if (processStart.isSuccess) {
      contextInitInfos(contextActorName) = (isAdHoc, successFunc, failureFunc)
    } else {
      failureFunc(processStart.failed.get)
    }

  }

  private def createContextDir(name: String,
                               contextConfig: Config,
                               isAdHoc: Boolean,
                               actorName: String): java.io.File = {
    // Create a temporary dir, preferably in the LOG_DIR
    val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")
    val tmpDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$encodedContextName")
    }.getOrElse(Files.createTempDirectory("jobserver"))
    logger.info("Created working directory {} for context {}", tmpDir: Any, name)

    // Now create the contextConfig merged with the values we need
    val mergedConfig = ConfigFactory.parseMap(
                         Map("is-adhoc" -> isAdHoc.toString,
                             "context.name" -> name,
                             "context.actorname" -> actorName).asJava
                       ).withFallback(contextConfig)

    // Write out the config to the temp dir
    Files.write(tmpDir.resolve("context.conf"),
                Seq(mergedConfig.root.render(ConfigRenderOptions.concise)).asJava,
                Charset.forName("UTF-8"))

    tmpDir.toFile
  }

  //generate remote context path and context config
  private def generateContext(name: String,
                              contextConfig: Config,
                              isAdHoc: Boolean,
                              actorName: String): (String, String) = {
    (Option(System.getProperty("LOG_DIR")).getOrElse("/tmp/jobserver") + "/"
      + java.net.URLEncoder.encode(name + "-" + actorName, "UTF-8"),
      ConfigFactory.parseMap(
        Map("is-adhoc" -> isAdHoc.toString,
          "context.name" -> name,
          "context.actorname" -> actorName).asJava
      ).withFallback(contextConfig).root().render(ConfigRenderOptions.concise())
      )
  }

  private def addContextsFromConfig(config: Config) {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, false) { ref => } {
          e => logger.error("Unable to start context" + contextName, e)
        }
      }
    }

  }
}
