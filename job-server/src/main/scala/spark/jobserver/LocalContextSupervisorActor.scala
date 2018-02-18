package spark.jobserver

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import spark.jobserver.JobManagerActor.{GetContexData, ContexData}
import spark.jobserver.JobManagerActor.{SparkContextAlive, SparkContextDead, SparkContextStatus}
import spark.jobserver.util.SparkJobUtils

import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor
import akka.pattern.gracefulStop
import org.joda.time.DateTime
import spark.jobserver.io.JobDAOActor.CleanContextJobInfos

/** Messages common to all ContextSupervisors */
object ContextSupervisor {
  // Messages/actions
  case object AddContextsFromConfig // Start up initial contexts
  case object ListContexts
  case class AddContext(name: String, contextConfig: Config)
  case class StartAdHocContext(classPath: String, contextConfig: Config)
  case class GetContext(name: String) // returns JobManager, JobResultActor
  case class GetResultActor(name: String)  // returns JobResultActor
  case class StopContext(name: String)
  case class GetSparkContexData(name: String)

  // Errors/Responses
  case object ContextInitialized
  case class ContextInitError(t: Throwable)
  case class ContextStopError(t: Throwable)
  case object ContextAlreadyExists
  case object NoSuchContext
  case object ContextStopped
  case class SparkContexData(name: String, appId: String, url: Option[String])
}

/**
 * This class starts and stops JobManagers / Contexts in-process.
 * It is responsible for watching out for the death of contexts/JobManagers.
 *
 * == Auto context start configuration ==
 * Contexts can be configured to be created automatically at job server initialization.
 * Configuration example:
 * {{{
 *   spark {
 *     contexts {
 *       olap-demo {
 *         num-cpu-cores = 4            # Number of cores to allocate.  Required.
 *         memory-per-node = 1024m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
 *       }
 *     }
 *   }
 * }}}
 *
 * == Other configuration ==
 * {{{
 *   spark {
 *     jobserver {
 *       context-creation-timeout = 15 s
 *       yarn-context-creation-timeout = 40 s
 *     }
 *
 *     # Default settings for all context creation
 *     context-settings {
 *       spark.mesos.coarse = true
 *     }
 *   }
 * }}}
 */
class LocalContextSupervisorActor(dao: ActorRef, dataManagerActor: ActorRef) extends InstrumentedActor {
  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextTimeout = SparkJobUtils.getContextCreationTimeout(config)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  import context.dispatcher   // to get ExecutionContext for futures

  private val contexts = mutable.HashMap.empty[String, (ActorRef, ActorRef)]

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  def wrappedReceive: Receive = {
    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      sender ! contexts.keys.toSeq

    case GetSparkContexData(name) =>
      contexts.get(name) match {
        case Some((actor, _)) =>
          val future = (actor ? GetContexData)(contextTimeout.seconds)
          val originator = sender
          future.collect {
            case ContexData(appId, Some(webUi)) =>
              originator ! SparkContexData(name, appId, Some(webUi))
            case ContexData(appId, None) => originator ! SparkContexData(name, appId, None)
            case SparkContextDead =>
              logger.info("SparkContext {} is dead", name)
              originator ! NoSuchContext
          }
        case _ => sender ! NoSuchContext
      }

    case AddContext(name, contextConfig) =>
      val originator = sender // Sender is a mutable reference, must capture in immutable val
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      if (contexts contains name) {
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

      // Keep generating context name till there is no collision
      var contextName = ""
      do {
        contextName = userNamePrefix +
          java.util.UUID.randomUUID().toString().substring(0, 8) + "-" + classPath
      } while (contexts contains contextName)

      // Create JobManagerActor and JobResultActor
      startContext(contextName, mergedConfig, true, contextTimeout) { contextMgr =>
        originator ! contexts(contextName)
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! contexts.get(name).map(_._2).getOrElse(globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        val future = (contexts(name)._1 ? SparkContextStatus) (contextTimeout.seconds)
        val originator = sender
        future.collect {
          case SparkContextAlive => originator ! contexts(name)
          case SparkContextDead =>
            logger.info("SparkContext {} is dead", name)
            self ! StopContext(name)
            originator ! NoSuchContext
        }
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info("Shutting down context {}", name)
        try {
          val stoppedCtx = gracefulStop(contexts(name)._1, contextDeletionTimeout seconds)
          Await.result(stoppedCtx, contextDeletionTimeout + 1 seconds)
          contexts.remove(name)
          sender ! ContextStopped
        }
        catch {
          case err: Exception => sender ! ContextStopError(err)
        }
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name = actorRef.path.name
      logger.info("Actor terminated: " + name)
      contexts.remove(name)
      dao ! CleanContextJobInfos(name, DateTime.now())
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean, timeoutSecs: Int = 1)
                          (successFunc: ActorRef => Unit)
                          (failureFunc: Throwable => Unit) {
    require(!(contexts contains name), "There is already a context named " + name)
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
        contexts(name) = (ref, resultActor)
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
