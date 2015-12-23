package spark.jobserver

import java.io.IOException
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberUp, MemberEvent, InitialStateAsEvents}
import akka.util.Timeout
import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.util.SparkJobUtils
import scala.collection.mutable
import scala.util.{Try, Success, Failure}
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
class AkkaClusterSupervisorActor(daoActor: ActorRef) extends InstrumentedActor {
  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextInitTimeout = config.getDuration("spark.context-settings.context-init-timeout",
                                                TimeUnit.SECONDS)
  val managerStartCommand = config.getString("deploy.manager-start-cmd")
  import context.dispatcher

  //actor name -> (context name, context config, context isadhoc)
  //TODO: try to pass this state to the jobManager at start instead of having to track
  //extra state.  What happens if the WebApi process dies before the forked process
  //starts up?  Then it never gets initialized, and this state disappears.
  private val contextInitInfos = mutable.HashMap.empty[String, (String, Config, Boolean)]
  //actor name -> (success callback, failure callback)
  private val contextCallbacks = mutable.HashMap.empty[String, (ActorRef => Unit, Throwable => Unit)]

  private val contexts = mutable.HashMap.empty[String, ActorRef]
  private val resultActors = mutable.HashMap.empty[String, ActorRef]

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
      actorRefOpt.map { actorRef =>
        val actorName = actorRef.path.name
        if (actorName.startsWith("jobManager")) {
          logger.info("Received identify response, attempting to initialize context at {}", memberActors)
          (for { (ctxName, ctxConf, isAdHoc) <- contextInitInfos.remove(actorName)
                 (successFunc, failureFunc)  <- contextCallbacks.remove(actorName) }
           yield {
             initContext(actorName, actorRef, contextInitTimeout)(
               ctxName, ctxConf, isAdHoc)(successFunc, failureFunc)
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
        originator ! (contexts(contextName), resultActors(contextName))
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! resultActors.get(name).getOrElse(globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        sender ! (contexts(name), resultActors(name))
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info("Shutting down context {}", name)

        context.watch(contexts(name))
        contexts(name) ! PoisonPill
        resultActors.remove(name)
        sender ! ContextStopped
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      logger.info("Actor terminated: {}", name)
      contexts.retain { case (k, v) => v != actorRef }
      resultActors.retain { case (k, v) => v != actorRef }
  }

  private def initContext(actorName: String, ref: ActorRef, timeoutSecs: Long = 1)
                         (ctxName: String, ctxConf: Config, isAdHoc: Boolean)
                         (successFunc: ActorRef => Unit, failureFunc: Throwable => Unit): Unit = {
    import akka.pattern.ask

    val resultActor = if (isAdHoc) globalResultActor else context.actorOf(Props(classOf[JobResultActor]))
    (ref ? JobManagerActor.Initialize(
      daoActor, Some(resultActor), ctxName, ctxConf, isAdHoc, self))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e:Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        ref ! PoisonPill
        failureFunc(e)
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        ref ! PoisonPill
        failureFunc(t)
      case Success(JobManagerActor.Initialized(_)) =>
        logger.info("SparkContext {} joined", ctxName)
        contexts(ctxName) = ref
        resultActors(ctxName) = resultActor
        successFunc(ref)
    }
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean)
                          (successFunc: ActorRef => Unit)(failureFunc: Throwable => Unit) = {
    require(!(contexts contains name), "There is already a context named " + name)
    val contextActorName = "jobManager-" + java.util.UUID.randomUUID().toString.substring(16)

    logger.info("Starting context with actor name {} ", contextActorName)

    val pb = Process(s"$managerStartCommand $contextActorName ${selfAddress.toString}")
    val pio = new ProcessIO(_ => (),
                        stdout => scala.io.Source.fromInputStream(stdout)
                          .getLines.foreach(println),
                        stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(println))
    logger.info("Starting to execute sub process {}", pb)
    val processStart = Try {
      val process = pb.run(pio)
      val exitVal = process.exitValue()
      if (exitVal != 0) {
        throw new IOException("Failed to launch context process, got exit code " + exitVal)
      }
    }

    if (processStart.isSuccess) {
      contextInitInfos(contextActorName) = (name, contextConfig, isAdHoc)
      contextCallbacks(contextActorName) = (successFunc, failureFunc)
    } else {
      failureFunc(processStart.failed.get)
    }

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
