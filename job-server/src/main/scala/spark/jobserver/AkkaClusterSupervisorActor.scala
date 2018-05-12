package spark.jobserver

import java.nio.file.{Files, Paths}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.util.{SparkJobUtils, ManagerLauncher}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

import scala.concurrent.Await
import akka.pattern.gracefulStop
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.JobManagerActor.{GetContexData, ContexData, SparkContextDead}
import spark.jobserver.io.{JobDAOActor, ContextInfo, ContextStatus}
import spark.jobserver.util.{InternalServerErrorException, NoCallbackFoundException}

object AkkaClusterSupervisorActor {
  val MANAGER_ACTOR_PREFIX = "jobManager-"
}

/**
 * The AkkaClusterSupervisorActor launches Spark Contexts as external processes
 * that connect back with the master node via Akka Cluster.
 *
 * Currently, when the Supervisor gets a MemberUp message from another actor,
 * it is assumed to be one starting up, and it will be asked to identify itself,
 * and then the Supervisor will try to initialize it.
 *
 * See the [[LocalContextSupervisorActor]] for normal config options.
 */
class AkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef)
    extends InstrumentedActor {

  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextInitTimeout = config.getDuration("spark.context-settings.context-init-timeout",
                                                TimeUnit.SECONDS)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  val daoAskTimeout = Timeout(config.getDuration("spark.jobserver.dao-timeout",
                                                TimeUnit.SECONDS).second)

  import context.dispatcher

  protected val contextInitInfos = mutable.HashMap.empty[String, (ActorRef => Unit, Throwable => Unit)]

  // actor name -> ResultActor ref
  private val resultActorRefs = mutable.HashMap.empty[String, ActorRef]
  private val jobManagerActorRefs = mutable.HashMap.empty[String, ActorRef]

  private val cluster = Cluster(context.system)
  protected val selfAddress = cluster.selfAddress

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)

  def getActorRef(contextInfo: ContextInfo) : Option[ActorRef] = {
    if (jobManagerActorRefs.exists(_._1 == contextInfo.id)) {
      Some(jobManagerActorRefs(contextInfo.id))
    } else if (contextInfo.actorAddress.nonEmpty) {
      val finiteDuration = FiniteDuration(3, SECONDS)
      val address = contextInfo.actorAddress.get + "/user/" +
          AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX + contextInfo.id
      try {
        val contextActorRefFuture = context.actorSelection(address).resolveOne(finiteDuration)
        jobManagerActorRefs(contextInfo.id) = Await.result(contextActorRefFuture, finiteDuration)
        Some(jobManagerActorRefs(contextInfo.id))
      } catch {
        case e: Exception =>
          logger.error("Failed to resolve reference for context " + contextInfo.name
              + " with exception " + e.getMessage)
          None
      }
    } else {
      logger.error("Reference for context " + contextInfo.name + " does not exist")
      None
    }
  }

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

          val contextId = actorName.replace(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX, "")
          val contextFromDAO =
            getDataFromDAO[JobDAOActor.ContextResponse](JobDAOActor.GetContextInfo(contextId))
          val callbacks = contextInitInfos.remove(actorName)
          (contextFromDAO, callbacks) match {
            case (Some(JobDAOActor.ContextResponse(Some(contextInfo))),
                Some((successCallback, failureCallback))) =>
              val contextConfig = ConfigFactory.parseString(contextInfo.config)
              val isAdhoc = contextConfig.getBoolean("is-adhoc")
              initContext(contextConfig, actorName,
                     actorRef, contextInitTimeout)(isAdhoc, successCallback, failureCallback)
            case (Some(JobDAOActor.ContextResponse(None)), _) =>
              logger.error(
                 s"No such contextId ${contextId} was found in DB. Cannot initialize actor ${actorName}")
              actorRef ! PoisonPill
            case (Some(JobDAOActor.ContextResponse(Some(contextInfo))), None) =>
              val exception = NoCallbackFoundException(contextInfo.id, actorRef.path.toSerializationFormat)
              logger.error(exception.getMessage, exception)
              actorRef ! PoisonPill // Since no watch was added, Terminated will not be fired
              daoActor ! JobDAOActor.SaveContextInfo(contextInfo.copy(
                  state = ContextStatus.Error, endTime = Some(DateTime.now()), error = Some(exception)))
            case (None, Some((_, failureCallback))) =>
              val exception = InternalServerErrorException(contextId)
              logger.error(exception.getMessage, exception)
              actorRef ! PoisonPill
              failureCallback(exception)
            case (None, None) =>
              val errorMessage = s"Failed to create context ($contextId) due to internal error"
              logger.error(errorMessage)
              actorRef ! PoisonPill
          }
        }
      }

    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      val resp = getDataFromDAO[JobDAOActor.ContextInfos](
          JobDAOActor.GetContextInfos(None, Some(ContextStatus.Running)))
      resp match {
        case Some(JobDAOActor.ContextInfos(contextInfos)) => sender ! contextInfos.map(_.name)
        case None => sender ! UnexpectedError
      }

    case GetSparkContexData(name) =>
      val originator = sender
      val resp = getContextByName(name)
      resp match {
        case Some(JobDAOActor.ContextResponse(Some(c))) =>
          val contextActorRef = getActorRef(c)
          contextActorRef match {
            case Some(ref) => val future = (ref ? GetContexData)(30.seconds)
              future.collect {
                case ContexData(appId, Some(webUi)) =>
                  originator ! SparkContexData(name, appId, Some(webUi))
                case ContexData(appId, None) =>
                  originator ! SparkContexData(name, appId, None)
                case SparkContextDead =>
                  logger.info("SparkContext {} is dead", name)
                  originator ! NoSuchContext
              }
            case None => sender ! NoSuchContext
          }
        case Some(JobDAOActor.ContextResponse(None)) => sender ! NoSuchContext
        case None => sender ! UnexpectedError
      }

    case AddContext(name, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      // TODO(velvia): This check is not atomic because contexts is only populated
      // after SparkContext successfully created!  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349
      val resp = getActiveContextByName(name)
      (resp._1, resp._2) match {
        case (true, Some(c)) => originator ! ContextAlreadyExists
        case (true, None) => startContext(name, mergedConfig, false) { ref =>
            originator ! ContextInitialized
          } { err =>
            originator ! ContextInitError(err)
          }
        case (false, _) => sender ! UnexpectedError
      }

    case StartAdHocContext(classPath, contextConfig) =>
      val originator = sender
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      val userNamePrefix = Try(mergedConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
        .map(SparkJobUtils.userNamePrefix(_)).getOrElse("")
      var contextName = userNamePrefix + java.util.UUID.randomUUID().toString() + "-" + classPath
      startContext(contextName, mergedConfig, true) { ref =>
        originator ! ref
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! resultActorRefs.get(name).getOrElse(globalResultActor)

    case GetContext(name) =>
      val resp = getContextByName(name)
      resp match {
        case Some(JobDAOActor.ContextResponse(Some(c))) =>
          val contextActorRef = getActorRef(c)
          contextActorRef match {
            case Some(ref) => sender ! ref
            case None => sender ! NoSuchContext
          }
        case Some(JobDAOActor.ContextResponse(None)) => sender ! NoSuchContext
        case None => sender ! UnexpectedError
      }

    case StopContext(name) =>
      val resp = getContextByName(name)
      resp match {
        case Some(JobDAOActor.ContextResponse(Some(c))) =>
          logger.info("Shutting down context {}", name)
          val contextInfo = ContextInfo(c.id, c.name, c.config, c.actorAddress, c.startTime,
            c.endTime, ContextStatus.Stopping, c.error)
          daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
          val contextActorRef = getActorRef(c)
          contextActorRef match {
            case Some(ref) =>
              val address = AddressFromURIString(c.actorAddress.get)
              cluster.down(address)
              try {
                val stoppedCtx = gracefulStop(ref, contextDeletionTimeout seconds)
                Await.result(stoppedCtx, contextDeletionTimeout + 1 seconds)
                sender ! ContextStopped
              } catch {
                case err: Exception => sender ! ContextStopError(err)
              }
            case None => sender ! NoSuchContext
          }
        case Some(JobDAOActor.ContextResponse(None)) => sender ! NoSuchContext
        case None => sender ! UnexpectedError
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      logger.info("Actor terminated: {}", name)
      val contextId = name.split(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX).apply(1)
      val resp = getDataFromDAO[JobDAOActor.ContextResponse](JobDAOActor.GetContextInfo(contextId))
      resp match {
        case Some(JobDAOActor.ContextResponse(Some(c))) =>
          val state =
            if (c.state == ContextStatus.Stopping) ContextStatus.Finished else ContextStatus.Killed
          val contextInfo = ContextInfo(c.id, c.name, c.config, c.actorAddress, c.startTime,
                Option(DateTime.now()), state, c.error)
          daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
          daoActor ! JobDAOActor.CleanContextJobInfos(c.id, DateTime.now())
        case Some(JobDAOActor.ContextResponse(None)) =>
          logger.error("No context for deletion is found in the DB")
        case None =>
          logger.error("Error occurred after Terminated message was recieved")
      }
      cluster.down(actorRef.path.address)
      jobManagerActorRefs.remove(contextId)
  }

  private def initContext(contextConfig: Config,
                          actorName: String,
                          ref: ActorRef,
                          timeoutSecs: Long = 1)
                         (isAdHoc: Boolean,
                          successFunc: ActorRef => Unit,
                          failureFunc: Throwable => Unit): Unit = {
    import akka.pattern.ask
    val resultActor = if (isAdHoc) globalResultActor else context.actorOf(Props(classOf[JobResultActor]))
    (ref ? JobManagerActor.Initialize(
      contextConfig, Some(resultActor), dataManagerActor))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e: Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        cluster.down(ref.path.address)
        ref ! PoisonPill
        failureFunc(e)
        initContextHelp(actorName, Some(ref.path.address.toString), ContextStatus.Error, Some(e))
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        cluster.down(ref.path.address)
        ref ! PoisonPill
        failureFunc(t)
        initContextHelp(actorName, Some(ref.path.address.toString), ContextStatus.Error, Some(t))
      case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        resultActorRefs(ctxName) = resActor
        context.watch(ref)
        initContextHelp(actorName, Some(ref.path.address.toString), ContextStatus.Running, None) match {
          case None => successFunc(ref)
          case Some(e) => ref ! PoisonPill
            failureFunc(e)
        }
      case _ => logger.info("Failed for unknown reason.")
        cluster.down(ref.path.address)
        ref ! PoisonPill
        val e = new RuntimeException("Failed for unknown reason.")
        failureFunc(e)
        initContextHelp(actorName, Some(ref.path.address.toString), ContextStatus.Error, Some(e))
    }
  }

  private def initContextHelp(actorName: String, clusterAddress: Option[String],
      state: String, error: Option[Throwable]) : Option[Throwable] = {
    import akka.pattern.ask
    val managerActorName = actorName.replace(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX, "")

    val resp = getDataFromDAO[JobDAOActor.ContextResponse](JobDAOActor.GetContextInfo(managerActorName))
    resp match {
      case Some(JobDAOActor.ContextResponse(Some(context))) =>
                    val endTime = error match {
            case None => context.endTime
            case _ => Some(DateTime.now())
          }
          daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(context.id,
                                                  context.name,
                                                  context.config,
                                                  clusterAddress,
                                                  context.startTime,
                                                  endTime,
                                                  state,
                                                  error))
          None
      case Some(JobDAOActor.ContextResponse(None)) =>
          val e = new Throwable("Did not find context in the DB")
          logger.error("Could not find context with id: " + managerActorName
              + ", received following error message: " + e.getMessage)
          Some(e)
      case None =>
        logger.error("Error occurred while fetching data from DAO.")
        Some(new Exception("Internal server error"))
    }
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean)
                          (successFunc: ActorRef => Unit)(failureFunc: Throwable => Unit): Unit = {

    val contextId = java.util.UUID.randomUUID().toString
    val contextActorName = AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX + contextId

    logger.info("Starting context with actor name {}", contextActorName)

    // Now create the contextConfig merged with the values we need
    val mergedContextConfig = ConfigFactory.parseMap(
      Map("is-adhoc" -> isAdHoc.toString, "context.name" -> name).asJava
    ).withFallback(contextConfig)
    val contextInfo = ContextInfo(contextId, name,
        mergedContextConfig.root().render(ConfigRenderOptions.concise()), None,
        DateTime.now(), None, _: String, _: Option[Throwable])
    launchDriver(name, contextConfig, contextActorName) match {
      case (false, error) => val e = new Exception(error)
        failureFunc(e)
        daoActor ! JobDAOActor.SaveContextInfo(contextInfo(ContextStatus.Error, Some(e)))
      case (true, _) =>
        contextInitInfos(contextActorName) = (successFunc, failureFunc)
        daoActor ! JobDAOActor.SaveContextInfo(contextInfo(ContextStatus.Started, None))
    }
  }

  protected def launchDriver(name: String, contextConfig: Config, contextActorName: String):
        (Boolean, String) = {
    // Create a temporary dir, preferably in the LOG_DIR
    val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")
    val contextDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$encodedContextName")
    }.getOrElse(Files.createTempDirectory("jobserver"))
    logger.info("Created working directory {} for context {}", contextDir: Any, name)

    val launcher = new ManagerLauncher(config, contextConfig,
        selfAddress.toString, contextActorName, contextDir.toString)

    launcher.start()
  }

  private def addContextsFromConfig(config: Config) {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val resp = getActiveContextByName(contextName)
        (resp._1, resp._2) match {
          case (true, Some(c)) =>
            val contextInfo = ContextInfo(c.id, c.name, c.config, c.actorAddress, c.startTime,
              Some(DateTime.now()), ContextStatus.Error,
              Some(new Throwable("Context was not finished properly")))
            daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
          case (_, _) =>
        }
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, false) { ref => } {
          e => logger.error("Unable to start context" + contextName, e)
        }
      }
    }
  }

  protected def getDataFromDAO[T: ClassTag](msg: JobDAOActor.JobDAORequest): Option[T] = {
    Try(Some(Await.result((daoActor ? msg)(daoAskTimeout).mapTo[T], daoAskTimeout.duration))).getOrElse(None)
  }

  private def getContextByName(name: String): Option[JobDAOActor.ContextResponse] = {
      getDataFromDAO[JobDAOActor.ContextResponse](JobDAOActor.GetContextInfoByName(name))
  }

  private def getActiveContextByName(name: String): (Boolean, Option[ContextInfo]) = {
    val resp = getContextByName(name)
    resp match {
      case None => (false, None)
      case Some(JobDAOActor.ContextResponse(Some(c)))
        if c.state == ContextStatus.Running || c.state == ContextStatus.Started => (true, Some(c))
      case Some(_) => (true, None)
    }
  }
}
