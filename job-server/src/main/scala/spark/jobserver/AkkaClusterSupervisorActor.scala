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
import spark.jobserver.JobManagerActor.{GetContexData, ContexData, SparkContextDead, RestartExistingJobs}
import spark.jobserver.JobManagerActor.ContextTerminatedException
import spark.jobserver.io.{JobDAOActor, ContextInfo, ContextStatus, JobStatus, ErrorData}
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
    contextInfo.actorAddress match {
      case Some(address) =>
        val actorPath = contextInfo.actorAddress.get + "/user/" +
          AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX + contextInfo.id
        jobManagerActorRefs.exists(_._1 == actorPath) match {
          case true => Some(jobManagerActorRefs(actorPath))
          case false =>
            val finiteDuration = FiniteDuration(3, SECONDS)
            try {
              val contextActorRefFuture = context.actorSelection(actorPath).resolveOne(finiteDuration)
              jobManagerActorRefs(actorPath) = Await.result(contextActorRefFuture, finiteDuration)
              Some(jobManagerActorRefs(actorPath))
            } catch {
              case e: Exception =>
                logger.error("Failed to resolve reference for context " + contextInfo.name
                    + " with exception " + e.getMessage)
                None
            }
          }
      case None =>
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
              initContext(contextInfo, actorName, actorRef, false)(successCallback, failureCallback)
            case (Some(JobDAOActor.ContextResponse(None)), _) =>
              logger.error(
                 s"No such contextId ${contextId} was found in DB. Cannot initialize actor ${actorName}")
              actorRef ! PoisonPill
            case (Some(JobDAOActor.ContextResponse(Some(contextInfo))), None) =>
              isSuperviseModeEnabled(config, ConfigFactory.parseString(contextInfo.config)) match {
                case isRestartScenario @ true =>
                  logger.info(
                     s"Restart request for context (${contextId}) received, probably due to supervise mode")
                  initContext(contextInfo, actorName, actorRef, isRestartScenario)(
                     { ref =>
                       logger.info(s"Successfully reinitialized context (${contextId}) under supervise mode")
                     },
                     { ref =>
                         logger.error(s"Failed to reinitialize context (${contextId}) under supervise mode")
                     })
                case false =>
                  val exception =
                    NoCallbackFoundException(contextInfo.id, actorRef.path.toSerializationFormat)
                  logger.error(exception.getMessage, exception)
                  actorRef ! PoisonPill // Since no watch was added, Terminated will not be fired
                  daoActor ! JobDAOActor.SaveContextInfo(contextInfo.copy(
                      state = ContextStatus.Error, endTime = Some(DateTime.now()), error = Some(exception)))
              }
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
          JobDAOActor.GetContextInfos(None, Some(
              Seq(ContextStatus.Running, ContextStatus.Restarting))))
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
      handleTerminatedEvent(actorRef)
  }

  protected def handleTerminatedEvent(actorRef: ActorRef) {
    val name: String = actorRef.path.name
    logger.info("Actor terminated: {}", name)
    val contextId = name.split(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX).apply(1)
    val resp = getDataFromDAO[JobDAOActor.ContextResponse](JobDAOActor.GetContextInfo(contextId))
    resp match {
      case Some(JobDAOActor.ContextResponse(Some(c))) =>
        val state =
                if (c.state == ContextStatus.Stopping) ContextStatus.Finished else ContextStatus.Killed
        (isSuperviseModeEnabled(config, ConfigFactory.parseString(c.config)), state) match {
            case (true, ContextStatus.Killed) =>
              setRestartingStateForContextAndJobs(c)
            case _ =>
              val contextInfo = c.copy(endTime = Option(DateTime.now()), state = state)
              daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
              daoActor ! JobDAOActor.CleanContextJobInfos(c.id, DateTime.now())
         }
      case Some(JobDAOActor.ContextResponse(None)) =>
        logger.error(s"No context (contextId: ${contextId}) for deletion is found in the DB")
      case None =>
        logger.error(s"Error occurred after Terminated message was recieved for (contextId: ${contextId})")
    }
    cluster.down(actorRef.path.address)
    jobManagerActorRefs.remove(actorRef.path.toString())
  }

  private def setRestartingStateForContextAndJobs(contextInfo: ContextInfo) {
    val restartingContext = contextInfo.copy(endTime = Option(DateTime.now()),
        state = ContextStatus.Restarting)
    logger.info(s"Updating the status to Restarting for context ${contextInfo.id} and jobs within")
    daoActor ! JobDAOActor.SaveContextInfo(restartingContext)
    setRestartingStateForRunningJobs(contextInfo)
  }

  private def setRestartingStateForRunningJobs(contextInfo: ContextInfo) {
    (daoActor ? JobDAOActor.GetJobInfosByContextId(
        contextInfo.id, Some(Seq(JobStatus.Running))))(daoAskTimeout).onComplete {
      case Success(JobDAOActor.JobInfos(Seq())) =>
        logger.info(s"No jobs found for context ${contextInfo.name}, not updating status")
      case Success(JobDAOActor.JobInfos(jobInfos)) =>
        val error = ErrorData(JobManagerActor.ContextTerminatedException(contextInfo.id))
        jobInfos.foreach { jobInfo =>
          logger.info(s"Found job ${jobInfo.jobId} for context. Setting state to Restarting.")
          daoActor ! JobDAOActor.SaveJobInfo(jobInfo.copy(state = JobStatus.Restarting,
              endTime = Some(DateTime.now()), error = Some(error)))
        }
      case Failure(e: Exception) =>
        logger.error(s"Exception occured while fetching jobs for context (${contextInfo.id})", e)
      case unexpectedMsg @ _ =>
        logger.error(s"$unexpectedMsg message received while fetching jobs for context (${contextInfo.id})")
    }
  }

  private def initContext(contextConfig: Config,
                          actorName: String,
                          ref: ActorRef,
                          timeoutSecs: Long = 1,
                          isRestartScenario: Boolean)
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
        (initContextHelp(actorName, Some(ref.path.address.toString), ContextStatus.Running, None),
            isRestartScenario) match {
          case (None, false) => successFunc(ref)
          case (None, true) =>
            logger.info("Context initialized, trying to restart existing jobs.")
            ref ! RestartExistingJobs
          case (Some(e), _) =>
            ref ! PoisonPill
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
    val statesInWhichContextCanBeActive =
      List(ContextStatus.Started, ContextStatus.Running, ContextStatus.Restarting)
    resp match {
      case None => (false, None)
      case Some(JobDAOActor.ContextResponse(Some(c)))
        if statesInWhichContextCanBeActive.contains(c.state) => (true, Some(c))
      case Some(_) => (true, None)
    }
  }

  private def isSuperviseModeEnabled(config: Config, contextConfig: Config): Boolean = {
    val defaultSuperviseMode = config.getBoolean(ManagerLauncher.SJS_SUPERVISE_MODE_KEY)
    val contextSuperviseMode = Try(
        Some(contextConfig.getBoolean(ManagerLauncher.CONTEXT_SUPERVISE_MODE_KEY))).getOrElse(None)
    ManagerLauncher.shouldSuperviseModeBeEnabled(defaultSuperviseMode, contextSuperviseMode)
  }

  private def initContext(contextInfo: ContextInfo, actorName: String, actorRef: ActorRef,
      isRestartScenario: Boolean)
      (successCallback: ActorRef => Unit, failureCallback: Throwable => Unit): Unit = {
    val contextConfig = ConfigFactory.parseString(contextInfo.config)
    val isAdhoc = contextConfig.getBoolean("is-adhoc")
    initContext(contextConfig, actorName,
           actorRef, contextInitTimeout, isRestartScenario)(isAdhoc, successCallback, failureCallback)
  }
}
