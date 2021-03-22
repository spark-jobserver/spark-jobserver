package spark.jobserver

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import akka.pattern.{ask, gracefulStop}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.JobManagerActor._
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.io.ContextInfo
import spark.jobserver.io.JobDAOActor.CleanContextJobInfos
import spark.jobserver.util.SparkJobUtils

import java.time.ZonedDateTime
import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

/**
 * This class starts and stops JobManagers / Contexts in-process.
 * It is responsible for watching out for the death of contexts/JobManagers.
 * This class is mainly used for testing and development as running multiple spark contexts
 * in parallel is *not* possible.
 */
class InProcessContextSupervisorActor(dao: ActorRef, dataManagerActor: ActorRef) extends InstrumentedActor {
  import ContextSupervisor._

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextTimeout = SparkJobUtils.getContextCreationTimeout(config)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  import context.dispatcher   // to get ExecutionContext for futures

  case class RunningContext(name: String, managerActor: ActorRef, resultActor: ActorRef)
  private var runningContext: Option[RunningContext] = None

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  def wrappedReceive: Receive = {
    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      logger.info("Running contexts " + runningContext.map(_.name).toSeq)
      sender ! runningContext.map(_.name).toSeq

    case GetSparkContexData(name) =>
      runningContext.filter(_.name == name) match {
        case Some(context) =>
          val future = (context.managerActor ? GetContexData)(contextTimeout.seconds)
          val originator = sender
          future.collect {
            case ContexData(appId, Some(webUi)) =>
              originator ! SparkContexData(name, Some(appId), Some(webUi))
            case ContexData(appId, None) => originator ! SparkContexData(name, Some(appId), None)
            case SparkContextDead =>
              logger.info("SparkContext {} is dead", name)
              originator ! NoSuchContext
          }
        case _ => sender ! NoSuchContext
      }

    case AddContext(name, contextConfig) =>
      val originator = sender // Sender is a mutable reference, must capture in immutable val
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      if (runningContext.exists(_.name == name)) {
        originator ! ContextAlreadyExists
      } else {
        startContext(name, mergedConfig, false, contextTimeout) { contextMgr =>
          originator ! ContextInitialized
        } { err =>
          originator ! ContextInitError(err)
        }
      }

    case StartAdHocContext(classPath, contextConfig) =>
      val originator = sender // Sender is a mutable reference, must capture in immutable val
      logger.info("Creating SparkContext for adhoc jobs.")

      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      val userNamePrefix = Try(mergedConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
        .map(SparkJobUtils.userNamePrefix(_)).getOrElse("")

      val contextName = s"$userNamePrefix-${java.util.UUID.randomUUID().toString.substring(0, 8)}-$classPath"

      // Create JobManagerActor and JobResultActor
      startContext(contextName, mergedConfig, true, contextTimeout) { contextMgr =>
        originator ! runningContext.map(ctx => (ctx.managerActor, ctx.resultActor)).get
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! runningContext.filter(_.name == name).map(_.resultActor).getOrElse(globalResultActor)

    case GetContext(name) =>
      println(runningContext)
      if (runningContext.exists(_.name == name)) {
        val future = (runningContext.map(_.managerActor).get ? SparkContextStatus) (contextTimeout.seconds)
        val originator = sender
        future.collect {
          case SparkContextAlive => originator ! runningContext.map(_.managerActor).get
          case SparkContextDead =>
            logger.info("SparkContext {} is dead", name)
            self ! StopContext(name)
            originator ! NoSuchContext
        }
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name, force) =>
      if (runningContext.exists(_.name == name)) {
        logger.info("Shutting down context {}", name)
        try {
          val stoppedCtx = gracefulStop(runningContext.get.managerActor, contextDeletionTimeout seconds)
          Await.result(stoppedCtx, contextDeletionTimeout + 5 seconds)
          runningContext = None
          sender ! ContextStopped
        }
        catch {
          case err: Exception => {
            sender ! ContextStopError(err)
          }
        }
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name = actorRef.path.name
      logger.info("Actor terminated: " + name)
      runningContext = None
      dao ! CleanContextJobInfos(name, ZonedDateTime.now())
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean, timeoutSecs: Int = 1)
                          (successFunc: ActorRef => Unit)
                          (failureFunc: Throwable => Unit) {
    if (runningContext.isDefined) {
      failureFunc(new IllegalArgumentException("Spark does not support multiple contexts per JVM." +
        "Running context: " + runningContext.map(_.name).getOrElse("Not found")))
      return
    }
    logger.info("Creating a SparkContext named {}", name)

    val resultActorRef = if (isAdHoc) Some(globalResultActor) else None
    val mergedConfig = ConfigFactory.parseMap(
                         Map("is-adhoc" -> isAdHoc.toString, "context.name" -> name).asJava
                       ).withFallback(contextConfig)
    val ref = context.actorOf(JobManagerActor.props(dao), name)
    (ref ? JobManagerActor.Initialize(
      mergedConfig, resultActorRef, dataManagerActor))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e: Exception) =>
        logger.error("Exception after sending Initialize to JobManagerActor", e)
        // Make sure we try to shut down the context in case it gets created anyways
        ref ! PoisonPill
        failureFunc(e)
      case Success(JobManagerActor.Initialized(_, resultActor)) =>
        logger.info("SparkContext {} initialized", name)
        runningContext = Some(RunningContext(name, ref, resultActor))
        context.watch(ref)
        successFunc(ref)
      case Success(JobManagerActor.InitError(t)) =>
        ref ! PoisonPill
        failureFunc(t)
      case x =>
        logger.warn("Unexpected message received by startContext: {}", x)
    }
  }

  // Adds the contexts from the config file
  private def addContextsFromConfig(config: Config) {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, false, contextTimeout) { ref => } {
          e => logger.error("Unable to start context " + contextName, e)
        }
        Thread sleep 500 // Give some spacing so multiple contexts can be created
      }
    }
  }

}
