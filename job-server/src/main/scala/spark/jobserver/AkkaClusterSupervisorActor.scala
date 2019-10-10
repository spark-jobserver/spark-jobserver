package spark.jobserver

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberUp}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.util._

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

import scala.concurrent.Await
import org.joda.time.DateTime
import spark.jobserver.JobManagerActor.{ContexData, GetContexData, RestartExistingJobs, SparkContextDead}
import spark.jobserver.io._
import com.google.common.annotations.VisibleForTesting
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings,
  ClusterSingletonProxy, ClusterSingletonProxySettings}

object AkkaClusterSupervisorActor {
  val MANAGER_ACTOR_PREFIX = "jobManager-"
  val ACTOR_NAME = "context-supervisor"

  def props(daoActor : ActorRef, dataManager : ActorRef, cluster : Cluster) : Props =
    Props(classOf[AkkaClusterSupervisorActor], daoActor, dataManager, cluster)

  def managerProps(daoActor : ActorRef, dataManager : ActorRef, cluster : Cluster) : Props =
    ClusterSingletonManager.props(
    singletonProps = props(daoActor, dataManager, cluster),
    terminationMessage = PoisonPill,
    settings = ClusterSingletonManagerSettings(cluster.system))

  def proxyProps(system: ActorSystem) : Props = ClusterSingletonProxy.props(
    singletonManagerPath = "/user/singleton",
    settings = ClusterSingletonProxySettings(system))

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
class AkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef,
    cluster: Cluster) extends InstrumentedActor {

  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextInitTimeout = config.getDuration("spark.context-settings.context-init-timeout",
                                                TimeUnit.SECONDS)
  val forkedJVMInitTimeout = SparkJobUtils.getForkedJVMInitTimeout(config)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  val daoAskTimeout = Timeout(config.getDuration("spark.jobserver.dao-timeout",
                                                TimeUnit.SECONDS).second)

  import context.dispatcher

  protected val contextInitInfos = mutable.HashMap.empty[String,
    (ActorRef => Unit, Throwable => Unit, Cancellable)]

  // actor name -> ResultActor ref
  private val resultActorRefs = mutable.HashMap.empty[String, ActorRef]
  private val jobManagerActorRefs = mutable.HashMap.empty[String, ActorRef]

  protected val selfAddress = cluster.selfAddress

  private val FINAL_STATES = Set(ContextStatus.Error, ContextStatus.Finished, ContextStatus.Killed)

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  logger.info("Subscribing to MemberUp event")
  cluster.subscribe(self, classOf[MemberEvent])
  regainWatchOnExistingContexts()

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
    logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    cluster.leave(selfAddress)
    logger.warn(s"Unsubscribing and leaving cluster with address ${selfAddress}")
  }

  def regainWatchOnExistingContexts(){
    getDataFromDAO[JobDAOActor.ContextInfos](JobDAOActor.GetContextInfos(None,
        Some(Seq(ContextStatus.Running, ContextStatus.Stopping))))
    match {
      case Some(JobDAOActor.ContextInfos(contextInfos)) =>
        logger.info(s"Regaining watch on ${contextInfos.length} existing contexts.")
        contextInfos.flatMap(JobServer.getManagerActorRef(_, context.system)).foreach{
            ref =>
              logger.info(s"Regaining watch on existing context with address ${ref.path.address.toString}.")
              context.watch(ref)
        }
      case None =>
        logger.error("Error fetching contexts from database. Not regaining any watches.")
    }
  }

  def wrappedReceive: Receive = {
    case state: CurrentClusterState =>
      lazy val members = StringBuilder.newBuilder
      state.members.foreach(m => members.append(m.address.toString).append(" "))
      logger.info(s"Current Akka cluster has members ${members.toString()}")

      state.leader match {
        case None => logger.warn("No leader for current Akka cluster!")
        case Some(address) => logger.info(s"Current Akka leader has address ${address.toString}")
      }

      state.roleLeaderMap.foreach {
        case (key, Some(address)) => logger.info(s"Role/Address entry: $key/${address.toString}")
        case (key, None) => logger.info(s"Role/Address entry: $key/")
      }

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
            case (Some(JobDAOActor.ContextResponse(Some(contextInfo))), _)
                if isContextInFinalState(contextInfo) =>
              logger.info(s"Context forked JVM (${contextInfo.name}) already errored out. Killing it.")
              actorRef ! PoisonPill
            case (Some(JobDAOActor.ContextResponse(Some(contextInfo))), _)
                if contextInfo.state == ContextStatus.Stopping =>
              logger.info(s"Context (${contextInfo.name}) in ${ContextStatus.Stopping} state is joining" +
                s"the cluster. Killing it.")
              daoActor ! JobDAOActor.SaveContextInfo(contextInfo.copy(
                state = ContextStatus.Error, endTime = Some(DateTime.now()),
                error = Some(new StoppedContextJoinedBackException)))
              daoActor ! JobDAOActor.CleanContextJobInfos(contextInfo.id, DateTime.now())
              actorRef ! PoisonPill
            case (Some(JobDAOActor.ContextResponse(Some(contextInfo))),
                Some((successCallback, failureCallback, timeoutMsgCancelHandler))) =>
              cancelScheduledMessage(timeoutMsgCancelHandler)
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
            case (None, Some((_, failureCallback, timeoutMsgCancelHandler))) =>
              cancelScheduledMessage(timeoutMsgCancelHandler)
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
              Seq(ContextStatus.Running, ContextStatus.Restarting, ContextStatus.Stopping))))
      resp match {
        case Some(JobDAOActor.ContextInfos(contextInfos)) => sender ! contextInfos.map(_.name)
        case None => sender ! UnexpectedError
      }

    case GetSparkContexData(name) =>
      val originator = sender
      val resp = getContextByName(name)
      resp match {
        case Some(JobDAOActor.ContextResponse(Some(contextInfo))) =>
          val contextActorRef = getActorRef(contextInfo)
          contextActorRef match {
            case Some(ref) => val future = (ref ? GetContexData)(10.seconds)
              future.collect {
                case ContexData(appId, Some(webUi)) =>
                  originator ! SparkContexData(contextInfo, Some(appId), Some(webUi))
                case ContexData(appId, None) =>
                  originator ! SparkContexData(contextInfo, Some(appId), None)
                case SparkContextDead =>
                  originator ! SparkContexData(contextInfo, None, None)
              }.recover {
                case e: Exception => originator ! SparkContexData(contextInfo, None, None)
              }
            case None => originator ! SparkContexData(contextInfo, None, None)
          }
        case Some(JobDAOActor.ContextResponse(None)) => originator ! NoSuchContext
        case None => originator ! UnexpectedError
      }

    case AddContext(name, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
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

    case StopContext(name, force) =>
      val resp = getContextByName(name)
      resp match {
        case Some(JobDAOActor.ContextResponse(Some(c))) =>
          logger.info(s"Shutting down context $name with force=$force")
          if (ContextStatus.getFinalStates() contains c.state) {
            sender ! NoSuchContext
          } else {
            val state = ContextStatus.Stopping
            val contextInfo = ContextInfo(c.id, c.name, c.config, c.actorAddress, c.startTime,
              c.endTime, state, c.error)
            daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
            val contextActorRef = getActorRef(c)
            contextActorRef match {
              case Some(ref) =>
                val originalSender = sender
                val msg = force match {
                  case true => JobManagerActor.StopContextForcefully
                  case false => JobManagerActor.StopContextAndShutdown
                }
                (ref ? msg)(contextDeletionTimeout seconds).onComplete {
                  case Success(SparkContextStopped) =>
                    logger.info("Spark context stopped successfully. Shutting down the driver actor system")
                    originalSender ! ContextStopped
                  case Success(ContextStopInProgress) =>
                    logger.info("Failed to stop context within timeout. Stop is still in progress")
                    originalSender ! ContextStopInProgress
                  case Success(ContextStopError(e)) =>
                    logger.error(s"Context stopped (force=${force}) failed with message ${e.getMessage}")
                    originalSender ! ContextStopError(e)
                  case Success(unknownMessage) =>
                    logger.error(s"Received  unknown response type: $unknownMessage")
                    originalSender ! ContextStopError(
                      new Throwable(s"Received unknown response trying to stop the context."))
                  case Failure(t) =>
                    logger.error(s"Context stopped failed with message ${t.getMessage}")
                    originalSender ! ContextStopError(t)
                }
              case None =>
                val e = new ResolutionFailedOnStopContextException(c)
                logger.info(s"${e.getMessage} Setting context to error state and cleaning up jobs.")
                daoActor ! JobDAOActor.SaveContextInfo(contextInfo.copy(state = ContextStatus.Error,
                    endTime = Some(DateTime.now()), error = Some(e)))
                daoActor ! JobDAOActor.CleanContextJobInfos(c.id, DateTime.now())
                sender ! NoSuchContext
            }
          }
        case Some(JobDAOActor.ContextResponse(None)) => sender ! NoSuchContext
        case None => sender ! UnexpectedError
      }

    case Terminated(actorRef) =>
      handleTerminatedEvent(actorRef)

    case ForkedJVMInitTimeout(contextActorName, contextInfo: ContextInfo) => {
      contextInitInfos.get(contextActorName) match {
        case Some((successFunc, failureFunc, _)) =>
          logger.warn(s"Context forked JVM failed to initialize for actor $contextActorName")
          daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
          failureFunc(ContextJVMInitializationTimeout())
        case None =>
      }
    }

  }

  protected def handleTerminatedEvent(actorRef: ActorRef) {
    val name: String = actorRef.path.name
    logger.info("Actor terminated: {}", name)
    val contextId = name.split(AkkaClusterSupervisorActor.MANAGER_ACTOR_PREFIX).apply(1)
    val resp = getDataFromDAO[JobDAOActor.ContextResponse](JobDAOActor.GetContextInfo(contextId))
    resp match {
      case Some(JobDAOActor.ContextResponse(Some(c))) =>
        ContextStatus.getFinalStates().contains(c.state) match {
          case true => logger.warn(
            s"Terminated received for context (${c.name}) which is already in final state ${c.state}")
          case false =>
            val state =
              if (c.state == ContextStatus.Stopping) ContextStatus.Finished else ContextStatus.Killed
            (isSuperviseModeEnabled(config, ConfigFactory.parseString(c.config)), state) match {
              case (true, ContextStatus.Killed) =>
                setStateForContextAndJobs(c, ContextStatus.Restarting)
              case _ =>
                val contextInfo = c.copy(endTime = Option(DateTime.now()), state = state)
                daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
                daoActor ! JobDAOActor.CleanContextJobInfos(c.id, DateTime.now())
            }
        }
      case Some(JobDAOActor.ContextResponse(None)) =>
        logger.error(s"No context (contextId: ${contextId}) for deletion is found in the DB")
      case None =>
        logger.error(s"Error occurred after Terminated message was recieved for (contextId: ${contextId})")
    }
    leaveCluster(actorRef)
    jobManagerActorRefs.remove(actorRef.path.toString())
  }

  protected def leaveCluster(actorRef: ActorRef): Unit = {
    cluster.down(actorRef.path.address)
  }

  private def setStateForContextAndJobs(contextInfo: ContextInfo, state: String) {
    val newContextInfo = contextInfo.copy(endTime = Option(DateTime.now()),
        state = state)
    logger.info(s"Updating the status to ${state} for context ${contextInfo.id} and jobs within")
    daoActor ! JobDAOActor.SaveContextInfo(newContextInfo)
    setStateForRunningAndRestartingJobs(contextInfo, state)
  }

  private def setStateForRunningAndRestartingJobs(contextInfo: ContextInfo, state: String) {
    (daoActor ? JobDAOActor.GetJobInfosByContextId(
        contextInfo.id, Some(Seq(JobStatus.Running, JobStatus.Restarting))))(daoAskTimeout).onComplete {
      case Success(JobDAOActor.JobInfos(Seq())) =>
        logger.info(s"No jobs found for context ${contextInfo.name}, not updating status")
      case Success(JobDAOActor.JobInfos(jobInfos)) =>
        val error = ErrorData(JobManagerActor.ContextTerminatedException(contextInfo.id))
        jobInfos.foreach { jobInfo =>
          logger.info(s"Found job ${jobInfo.jobId} for context. Setting state to ${state}.")
          daoActor ! JobDAOActor.SaveJobInfo(jobInfo.copy(state = state,
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
          val contextInfo = ContextInfo(context.id,
                                            context.name,
                                            context.config,
                                            clusterAddress,
                                            context.startTime,
                                            context.endTime,
                                            state,
                                            error)
          error match {
            case None => daoActor ! JobDAOActor.SaveContextInfo(contextInfo)
            case _ => setStateForContextAndJobs(contextInfo, state)
          }
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
        DateTime.now(), _: Option[DateTime] , _: String, _: Option[Throwable])
    launchDriver(name, contextConfig, contextActorName) match {
      case (false, error) => val e = new Exception(error)
        failureFunc(e)
        daoActor ! JobDAOActor.SaveContextInfo(
            contextInfo(Some(DateTime.now()), ContextStatus.Error, Some(e)))
      case (true, _) =>
        val timeoutMsg = ForkedJVMInitTimeout(contextActorName,
             contextInfo(Some(DateTime.now()), ContextStatus.Error, Some(ContextJVMInitializationTimeout())))
        // Scheduler can throw IllegalStateException
        Try(context.system.scheduler.scheduleOnce(forkedJVMInitTimeout.seconds, self, timeoutMsg)) match {
          case Success(timeoutMsgCancelHandler) =>
            logger.info("Scheduling a time out message for forked JVM")
            daoActor ! JobDAOActor.SaveContextInfo(contextInfo(None, ContextStatus.Started, None))
            contextInitInfos(contextActorName) = (successFunc, failureFunc, timeoutMsgCancelHandler)
          case Failure(e) =>
            logger.error("Failed to schedule a time out message for forked JVM", e)
            daoActor ! JobDAOActor.SaveContextInfo(
                contextInfo(Some(DateTime.now()), ContextStatus.Error, Some(e)))
            val unusedCancellable = new Cancellable {
              def cancel(): Boolean = { return false }
              def isCancelled: Boolean = { return false }
            }
            contextInitInfos(contextActorName) = (successFunc, failureFunc, unusedCancellable)
        }
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

    val masterSeedNodes = Try {
      Utils.getHASeedNodes(config) match {
        case Nil => selfAddress.toString // for backward compatibility
        case seedNodes => seedNodes.map(_.toString).mkString(",")
      }
    }.getOrElse {
      logger.warn("Failed to get HA seed nodes, falling back to non-HA mode and using this node as master")
      selfAddress.toString
    }

    val launcher = new ManagerLauncher(config, contextConfig,
      masterSeedNodes, encodedContextName, contextActorName, contextDir.toString)

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
        if ContextStatus.getNonFinalStates().contains(c.state) => (true, Some(c))
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

  private def cancelScheduledMessage(cancelHandler: Cancellable) {
    cancelHandler.cancel()
    logger.info(s"Scheduled message has been cancelled: ${cancelHandler.isCancelled.toString()}")
  }

  @VisibleForTesting
  protected def isContextInFinalState(contextInfo: ContextInfo): Boolean = {
    ContextStatus.getFinalStates().contains(contextInfo.state)
  }
}
