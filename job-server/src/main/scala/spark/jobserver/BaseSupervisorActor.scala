package spark.jobserver

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import akka.actor._
import akka.cluster.Member
import akka.cluster.ClusterEvent.MemberUp
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.JobManagerActor.{SparkContextAlive, SparkContextDead, SparkContextStatus}

/** Messages common to all ContextSupervisors */
object ContextSupervisor {
  // Messages/actions
  case object AddContextsFromConfig // Start up initial contexts
  case object ListContexts
  case class AddContext(name: String, contextConfig: Config)
  case class StartAdHocContext(classPath: String, contextConfig: Config)
  case class GetContext(name: String) // returns JobManager, JobResultActor
  case class GetResultActor(name: String)  // returns JobResultActor
  case class StopContext(name: String, retry: Boolean = true)

  // Errors/Responses
  case object ContextInitialized
  case class ContextInitError(t: Throwable)
  case object ContextAlreadyExists
  case object NoSuchContext
  case object ContextStopped
}

/**
 * Abstract class that defines all the messages to be implemented
 * in other supervisors
 */
abstract class BaseSupervisorActor extends InstrumentedActor {
  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")

  import context.dispatcher   // to get ExecutionContext for futures
  import akka.pattern.ask

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  type SJSContext = (String, Option[ActorRef], Option[ActorRef])
  // actor name -> (String contextActorName, Option[JobManagerActor] ref, Option[ResultActor] ref)
  private val contexts = mutable.HashMap.empty[String, SJSContext]

  def wrappedReceive: Receive = {
    case MemberUp(member) =>
      onMemberUp(member)

    case ActorIdentity(memberActors, actorRefOpt) =>
      onActorIdentity(memberActors, actorRefOpt)

    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      sender ! listContexts()

    case AddContext(name, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      if (haveContext(name)) {
        originator ! ContextAlreadyExists
      } else {
        registerAndStartContext(name, mergedConfig, false)(
          (ref) => originator ! ContextInitialized,
          (err) => originator ! ContextInitError(err)
        )
      }

    case StartAdHocContext(classPath, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)

      var contextName = ""
      do {
        contextName = java.util.UUID.randomUUID().toString().take(8) + "-" + classPath
      } while (haveContext(contextName))
      registerAndStartContext(contextName, mergedConfig, true)(
        (ref) => originator ! getContext(contextName),
        (err) => originator ! ContextInitError(err)
      )

    case GetResultActor(name) =>
      if (haveContext(name)) {
        sender ! getContext(name)._3.getOrElse(globalResultActor)
      } else {
        sender ! globalResultActor
      }

    case GetContext(name) =>
      if (haveContext(name)) {
        val actorOpt = getContext(name)._2
        if (actorOpt.isDefined) {
          val future = (actorOpt.get ? SparkContextStatus)(getTimeout().seconds)
          val originator = sender
          future.collect {
            case SparkContextAlive => originator ! getContext(name)
            case SparkContextDead =>
              logger.info("SparkContext {} is dead", name)
              self ! StopContext(name)
              originator ! NoSuchContext
          }
        }
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name, retry) =>
      if (haveContext(name)) {
        val contextActorOpt = getContext(name)._2
        if (contextActorOpt.isDefined) {
          val contextActorRef = contextActorOpt.get
          logger.info("Shutting down context {}", name)
          onStopContext(contextActorRef)
          contextActorRef ! PoisonPill
          sender ! ContextStopped
        } else if (retry) {
          logger.info(
            "Context {} is not yet started, will try and stop after {} seconds",
            name,
            getTimeout()
          )
          // send another stopContext, but make the original sender appear as originator
          context.system.scheduler.scheduleOnce(
            getTimeout().seconds, self, StopContext(name, false)
          )(context.dispatcher, sender)
        } else {
          logger.error("Context {} never started and was not removed, unexpected!, stopping!")
          self ! PoisonPill
        }
      } else {
        // if not in retry, its possible something else removed the context
        // be safe and signal that the context was stopped
        if (retry) {
          sender ! NoSuchContext
        } else {
          sender ! ContextStopped
        }
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      logger.info("Actor terminated: {}", name)
      removeContext(name)
  }

  protected def haveContext(name: String): Boolean = contexts.contains(name)
  protected def getContext(name: String): SJSContext = contexts(name)
  protected def removeContext(actorName: String): Unit = {
    contexts.retain { case (_, (curName, _, _)) => actorName != curName }
  }
  protected def listContexts(): Seq[String] = contexts.keys.toSeq

  protected def addContextStarting(name: String, actorName: String) = {
    contexts(name) = (actorName, None, None)
  }
  protected def addContext(name: String, context: SJSContext) = {
    contexts(name) = context
  }

  protected def onMemberUp(member: Member): Unit
  protected def onActorIdentity(memberActors: Any, actorRefOpt: Option[ActorRef]): Unit
  protected def onStopContext(actor: ActorRef): Unit
  protected def getTimeout(): Long

  protected def startContext(
    name: String, actorName: String, contextConfig: Config, isAdHoc: Boolean
  )(successFunc: ActorRef => Unit, failureFunc: Throwable => Unit): Unit


  protected def buildActorName(): String = "jobManager-" + java.util.UUID.randomUUID().toString.substring(16)

  protected def registerAndStartContext(
    name: String, contextConfig: Config, isAdHoc: Boolean
  )(successFunc: ActorRef => Unit, failureFunc: Throwable => Unit): Unit = {
    logger.info("Creating a SparkContext named {}", name)
    val actorName = buildActorName()
    addContextStarting(name, actorName)
    startContext(name, actorName, contextConfig, isAdHoc)(successFunc, failureFunc)
  }

  protected def initContext(
    actorName: String, ref: ActorRef, timeoutSecs: Long = 1
  )(
    isAdHoc: Boolean,
    successFunc: ActorRef => Unit,
    failureFunc: Throwable => Unit
  ): Unit = {
    val resultActor = if (isAdHoc) globalResultActor else context.actorOf(Props(classOf[JobResultActor]))
    (ref ? JobManagerActor.Initialize(
      Some(resultActor)))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e:Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        removeContext(actorName)
        ref ! PoisonPill
        failureFunc(e)
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        removeContext(actorName)
        ref ! PoisonPill
        failureFunc(t)
      case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        // re-add the context, now with references
        addContext(ctxName, (actorName, Some(ref), Some(resActor)))
        context.watch(ref)
        successFunc(ref)
      case _ =>
        logger.info("Failed for unknown reason.")
        removeContext(actorName)
        ref ! PoisonPill
        failureFunc(new RuntimeException("Failed for unknown reason."))
    }
  }

  // sleep between context creation
  protected def createContextSleep(): Unit = {
    Thread.sleep(500)
  }


  protected def createMergedActorConfig(
    name: String, actorName: String, contextConfig: Config, isAdHoc: Boolean
  ): Config = {
    ConfigFactory.parseMap(
      Map(
        "is-adhoc" -> isAdHoc.toString,
        "context.name" -> name,
        "context.actorname" -> actorName
      ).asJava
    ).withFallback(contextConfig)
  }

  private def addContextsFromConfig(config: Config): Unit = {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        registerAndStartContext(contextName, contextConfig, false)(
          (ref) => Unit,
          (e) => logger.error("Unable to start context" + contextName, e)
        )
        createContextSleep()
      }
    }
  }

}
