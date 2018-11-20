package spark.jobserver

import java.io.File
import java.net.{URI, URL}
import java.util.concurrent.Executors._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.joda.time.DateTime
import org.scalactic._
import spark.jobserver.api.{DataFileCache, JobEnvironment}
import spark.jobserver.ContextSupervisor.{ContextStopError, ContextStopInProgress, SparkContextStopped}
import spark.jobserver.io._
import spark.jobserver.util._
import spark.jobserver.context._
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.util.{ContextForcefulKillTimeout, StandaloneForcefulKill}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import org.spark_project.guava.annotations.VisibleForTesting

object JobManagerActor {
  // Messages
  sealed trait ContextStopSchedule
  case class Initialize(contextConfig: Config, resultActorOpt: Option[ActorRef],
                        dataFileActor: ActorRef)
  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]], existingJobInfo: Option[JobInfo] = None)
  case class KillJob(jobId: String)
  case class JobKilledException(jobId: String) extends Exception(s"Job $jobId killed")
  case class ContextTerminatedException(contextId: String)
    extends Exception(s"Unexpected termination of context $contextId")
  case class ContextStopScheduledMsgTimeout(sender: ActorRef) extends ContextStopSchedule
  case class ContextStopForcefullyScheduledMsgTimeout(sender: ActorRef) extends ContextStopSchedule
  case object StopContextAndShutdown
  case object StopContextForcefully

  case object GetContextConfig
  case object SparkContextStatus
  case object GetContexData
  case object RestartExistingJobs

  case class DeleteData(name: String)

  // Results/Data
  case class ContextConfig(contextName: String, contextConfig: SparkConf, hadoopConfig: Configuration)
  case class Initialized(contextName: String, resultActor: ActorRef)
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)
  case class ContexData(appId: String, url: Option[String])
  case object SparkContextAlive
  case object SparkContextDead



  // Akka 2.2.x style actor props for actor creation
  def props(daoActor: ActorRef, supervisorActorAddress: String = "", contextId: String = "",
      initializationTimeout: FiniteDuration = 40.seconds): Props =
      Props(new JobManagerActor(daoActor, supervisorActorAddress, contextId, initializationTimeout))
}

/**
 * The JobManager actor supervises jobs running in a single SparkContext, as well as shared metadata.
 * It creates a SparkContext (or a StreamingContext etc. depending on the factory class)
 * It also creates and supervises a JobResultActor and JobStatusActor, although an existing JobResultActor
 * can be passed in as well.
 *
 * == contextConfig ==
 * {{{
 *  num-cpu-cores = 4         # Total # of CPU cores to allocate across the cluster
 *  memory-per-node = 512m    # -Xmx style memory string for total memory to use for executor on one node
 *  dependent-jar-uris = ["local://opt/foo/my-foo-lib.jar"]
 *                            # URIs for dependent jars to load for entire context
 *  context-factory = "spark.jobserver.context.DefaultSparkContextFactory"
 *  spark.mesos.coarse = true  # per-context, rather than per-job, resource allocation
 *  rdd-ttl = 24 h            # time-to-live for RDDs in a SparkContext.  Don't specify = forever
 *  is-adhoc = false          # true if context is ad-hoc context
 *  context.name = "sql"      # Name of context
 * }}}
 *
 * == global configuration ==
 * {{{
 *   spark {
 *     jobserver {
 *       max-jobs-per-context = 16      # Number of jobs that can be run simultaneously per context
 *     }
 *   }
 * }}}
 */
class JobManagerActor(daoActor: ActorRef, supervisorActorAddress: String, contextId: String,
    initializationTimeout: FiniteDuration) extends InstrumentedActor {

  import CommonMessages._
  import JobManagerActor._
  import context.{become, dispatcher}
  import collection.JavaConverters._

  val config: Config = context.system.settings.config
  private val maxRunningJobs = SparkJobUtils.getMaxRunningJobs(config)
  val executionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(newFixedThreadPool(maxRunningJobs))

  val daoAskTimeout = Timeout(config.getDuration("spark.jobserver.dao-timeout", TimeUnit.SECONDS).second)

  var jobContext: ContextLike = _
  var sparkEnv: SparkEnv = _

  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.

  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  private val jobCacheEnabled = Try(config.getBoolean("spark.job-cache.enabled")).getOrElse(false)
  private val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  // Use Spark Context's built in classloader when SPARK-1230 is merged.
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)

  // NOTE: Must be initialized after cluster joined
  private var contextConfig: Config = _
  private var contextName: String = _
  private var isAdHoc: Boolean = _
  private var stopContextSenderAndHandler: (Option[ActorRef], Option[Cancellable]) = (None, None)
  @VisibleForTesting
  protected var statusActor: ActorRef = _
  @VisibleForTesting
  protected var resultActor: ActorRef = _
  private var factory: SparkContextFactory = _
  private var remoteFileCache: RemoteFileCache = _
  @VisibleForTesting
  protected var totalJobsToRestart = 0
  private var totalJobsWhichFailedToRestart = 0

  // NOTE: Must be initialized after sparkContext is created
  private var jobCache: JobCache = _

  private val jobServerNamedObjects = new JobServerNamedObjects(context.system)

  if (isKillingContextOnUnresponsiveSupervisorEnabled) {
    logger.info(s"Sending identify message to supervisor at $supervisorActorAddress")
    context.setReceiveTimeout(initializationTimeout)
    context.actorSelection(supervisorActorAddress) ! Identify(1)
  }

  private def getEnvironment(_jobId: String): JobEnvironment = {
    val _contextCfg = contextConfig
    new JobEnvironment with DataFileCache {
      def jobId: String = _jobId
      def namedObjects: NamedObjects = jobServerNamedObjects
      def contextConfig: Config = _contextCfg
      def getDataFile(dataFile: String): File = {
        remoteFileCache.getDataFile(dataFile)
      }
    }
  }

  override def postStop() {
    logger.info("Doing final clean up in post stop!")
    Option(jobContext).foreach { context =>
      if (context.sparkContext.isStopped) {} else {
        logger.warn(s"""Post stop fired but spark context still not stopped.
Most likely due to an unhandled exception or master
sending PoisonPill in this class. Stopping context.""".stripMargin.replaceAll("\n", " "))
        context.stop()
      }
    }
  }

  // Handle external kill events (e.g. killed via YARN)
  private def sparkListener = new SparkListener() {
    override def onApplicationEnd(event: SparkListenerApplicationEnd) {
      logger.info("Got Spark Application end event, stopping job manger.")
      // Note: This message can be dropped if postStop() has already fired due to PoisonPill.
      // After PoisonPill, new messages cannot be scheduled.
      self ! SparkContextStopped
    }
  }

  private def isKillingContextOnUnresponsiveSupervisorEnabled: Boolean = {
    supervisorActorAddress.nonEmpty
  }

  def adhocStopReceive: Receive = {
    case StopContextAndShutdown =>
      // Do a blocking stop, since job is already finished or errored out. Stop should be quick.
      setCurrentContextState(ContextInfoModifiable(ContextStatus.Stopping))
      jobContext.stop()

    case SparkContextStopped =>
      logger.info("Adhoc context stopped. Killing myself")
      self ! PoisonPill
  }

  def forcefulStoppingStateReceive: Receive = commonHandlers.orElse {
    case ContextStopForcefullyScheduledMsgTimeout(stopRequestSender) =>
      logger.warn("Failed to stop context forcefully within in timeout.")
      become(stoppingStateReceive)
      stopRequestSender ! ContextStopError(ContextForcefulKillTimeout())

    case SparkContextStopped =>
      logger.info(s"Context $contextId stopped successfully")
      val (stopContextSender, stopContextForcefullyTimeoutMsgHandler) = stopContextSenderAndHandler
      stopContextForcefullyTimeoutMsgHandler.foreach{ handler =>
        if (handler.isCancelled) {} else {
          stopContextForcefullyTimeoutMsgHandler.foreach(_.cancel())
          stopContextSender.foreach(_ ! SparkContextStopped)
        }
      }
      self ! PoisonPill
  }

  def stoppingStateReceive: Receive = commonHandlers.orElse {
    // Initialize message cannot come in this state
    // - If already in stopping state then JVM is also initialized. Initialize won't come
    // - If restarts then whole JVM will restart and we will have a clean state
    case ContextStopScheduledMsgTimeout(stopRequestSender) =>
      logger.warn("Failed to stop context within in timeout. Stop is still in progress")
      stopRequestSender ! ContextStopInProgress

    case StopContextForcefully =>
      val originalSender = sender
      become(forcefulStoppingStateReceive)
      stopContextForcefullyHelper(originalSender)

    case SparkContextStopped =>
      logger.info(s"Context $contextId stopped successfully")
      val (stopContextSender, stopContextTimeoutMsgHandler) = stopContextSenderAndHandler
      stopContextTimeoutMsgHandler.foreach{ handler =>
        if (handler.isCancelled) {} else {
          stopContextTimeoutMsgHandler.foreach(_.cancel())
          stopContextSender.foreach(_ ! SparkContextStopped)
        }
      }
      self ! PoisonPill

    case StopContextAndShutdown =>
      logger.info("Context stop already in progress")
      sender ! ContextStopInProgress

    case StartJob(_, _, _, _, _) =>
      logger.warn("Tried to start job in stopping state. Not doing anything.")
      sender ! ContextStopInProgress

    case RestartExistingJobs =>
      // This message is sent after a watch is added. Terminated will be raised on SJS Master
      logger.warn("No point in restarting existing jobs as context is in stopping state." +
        " Will be killed automatically")

    case Terminated(actorRef) =>
      if (actorRef.path.name == "context-supervisor") {
        logger.warn(s"Supervisor actor (${actorRef.path.address.toString}) terminated!" +
          s" I will be killed automatically because context stop is in progress!")
      }
    case unexpectedMsg @ _ =>
      logger.warn(s"Received unknown message in stopping state $unexpectedMsg")
  }

  val commonHandlers: Receive = {
    case GetContexData =>
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          val appId = jobContext.sparkContext.applicationId
          val webUiUrl = jobContext.sparkContext.uiWebUrl
          val msg = if (webUiUrl.isDefined) {
            ContexData(appId, Some(webUiUrl.get))
          } else {
            ContexData(appId, None)
          }
          sender ! msg
        } catch {
          case _: Exception =>
            logger.error("SparkContext does not exist!")
            sender ! SparkContextDead
        }
      }

    case KillJob(jobId: String) =>
      jobContext.sparkContext.cancelJobGroup(jobId)
      val resp = JobKilled(jobId, DateTime.now())
      statusActor ! resp
      sender ! resp

    case DeleteData(name: String) => remoteFileCache.deleteDataFile(name)
  }

  def wrappedReceive: Receive = commonHandlers.orElse {
    case ActorIdentity(memberActors, supervisorActorRef) =>
      supervisorActorRef.foreach { ref =>
        val actorName = ref.path.name
        if (actorName == "context-supervisor") {
          logger.info("Received supervisor's response for Identify message. Adding a watch.")
          context.watch(ref)

          logger.info("Waiting for Initialize message from master.")
        }
      }

    case Terminated(actorRef) =>
      if (actorRef.path.name == "context-supervisor") {
        logger.warn(s"Supervisor actor (${actorRef.path.address.toString}) terminated!" +
            s" Killing myself (${self.path.address.toString})!")
        self ! PoisonPill
      }

    case ReceiveTimeout =>
        logger.warn("Did not receive ActorIdentity/Initialized message from master." +
           s" Killing myself (${self.path.address.toString})!")
        self ! PoisonPill

    case Initialize(ctxConfig, resOpt, dataManagerActor) =>
      if (isKillingContextOnUnresponsiveSupervisorEnabled) {
        logger.info("Initialize message received from master, stopping the timer.")
        context.setReceiveTimeout(Duration.Undefined) // Deactivate receive timeout
      }

      contextConfig = ctxConfig
      logger.info("Starting context with config:\n" + contextConfig.root.render)
      contextName = contextConfig.getString("context.name")
      isAdHoc = Try(contextConfig.getBoolean("is-adhoc")).getOrElse(false)
      statusActor = context.actorOf(JobStatusActor.props(daoActor))
      resultActor = resOpt.getOrElse(context.actorOf(Props[JobResultActor]))
      remoteFileCache = new RemoteFileCache(self, dataManagerActor)

      try {
        // Load side jars first in case the ContextFactory comes from it
        getSideJars(contextConfig).foreach { jarUri =>
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
        }
        factory = getContextFactory
        jobContext = factory.makeContext(config, contextConfig, contextName)
        jobContext.sparkContext.addSparkListener(sparkListener)
        sparkEnv = SparkEnv.get
        jobCache = new JobCacheImpl(jobCacheSize, daoActor, jobContext.sparkContext, jarLoader)
        getSideJars(contextConfig).foreach { jarUri => jobContext.sparkContext.addJar(jarUri) }
        sender ! Initialized(contextName, resultActor)
      } catch {
        case t: Throwable =>
          logger.error("Failed to create context " + contextName + ", shutting down actor", t)
          sender ! InitError(t)
          self ! PoisonPill
      }

    case StartJob(appName, classPath, jobConfig, events, existingJobInfo) =>
      val loadedJars = jarLoader.getURLs
      getSideJars(jobConfig).foreach { jarUri =>
        val jarToLoad = new URL(convertJarUriSparkToJava(jarUri))
        if(! loadedJars.contains(jarToLoad)){
          logger.info("Adding {} to Current Job Class path", jarUri)
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
          jobContext.sparkContext.addJar(jarUri)
        }
      }
      startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv, existingJobInfo)

    case StopContextAndShutdown =>
      val originalSender = sender()
      logger.info("Shutting down SparkContext {}", contextName)
      Option(jobContext) match {
        case Some(_context) =>
          val cancelHandler = scheduleContextStopTimeoutMsg(originalSender)
          stopContextSenderAndHandler = (Some(originalSender), cancelHandler)
          Future {
            _context.stop()
          }
          become(stoppingStateReceive)
        case None =>
          logger.warn("Context was null, killing myself")
          originalSender ! SparkContextStopped
          self ! PoisonPill
      }

    case StopContextForcefully =>
      val originalSender = sender
      become(forcefulStoppingStateReceive)
      stopContextForcefullyHelper(originalSender)

    // Only used in LocalContextSupervisorActor
    case SparkContextStatus =>
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          jobContext.sparkContext.getSchedulingMode
          sender ! SparkContextAlive
        } catch {
          case e: Exception =>
            logger.error("SparkContext does not exist!", e)
            sender ! SparkContextDead
        }
      }

    case GetContextConfig =>
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          val conf: SparkConf = jobContext.sparkContext.getConf
          val hadoopConf: Configuration = jobContext.sparkContext.hadoopConfiguration
          sender ! ContextConfig(jobContext.sparkContext.appName, conf, hadoopConf)
        } catch {
          case e: Exception =>
            logger.error("SparkContext does not exist!")
            sender ! SparkContextDead
        }
      }

    case RestartExistingJobs =>
      logger.info("Job restart message received, trying to restart existing jobs.")
      restartTerminatedJobs(contextId, sender)

    /**
     * Normally, JobStarted/JobValidationFailed are sent back to WebAPI but in restart scenario,
     * this class will receive these messages. This class only handles the following
     * messages because we are subscribed to only these during restart.
     *
     * Other possible messages like JobErroredOut/JobFinished/JobKilled relate more to
     * what happens after the job was restarted. These messages won't be received by this actor,
     * since we are not subscribed but the statusActor will update DAO based on these messages.
     */
    case JobStarted(jobId, jobInfo) =>
      logger.info(s"Job ($jobId) restarted successfully")

    case msg @ JobValidationFailed(jobId, dateTime, error) =>
      handleJobRestartFailure(jobId, error, msg)

    /**
     * This message is specific to restart scenario. It is sent for all the errors before
     * StartJob message is sent to restart a job. All the messages after StartJob are
     * handled by JobStarted/JobValidationFailed
     */
    case msg @ JobRestartFailed(jobId, error) =>
      handleJobRestartFailure(jobId, error, msg)

    case SparkContextStopped =>
      // Stop context was not called but due to some external actions onApplicationEnd was fired
      logger.info("Got Spark Application end event externally, stopping job manager")
      // Even if the DAO request fails, we still kill our self because what else can we do at
      // this point? So, we just kill our self to release the resources.
      setCurrentContextState(ContextInfoModifiable(ContextStatus.Killed), killMyself(), killMyself())
  }

  private def killMyself(): Unit = {self ! PoisonPill}

  private def setCurrentContextState(attributes: ContextModifiableAttributes,
                             successCallback: => Unit = () => Unit,
                             failureCallback: => Unit = () => Unit) {
    (daoActor ? JobDAOActor.UpdateContextById(contextId, attributes))(daoAskTimeout)
        .mapTo[JobDAOActor.SaveResponse].onComplete {
      case Success(JobDAOActor.SavedSuccessfully) => successCallback
      case Success(JobDAOActor.SaveFailed(t)) =>
        logger.error(s"Failed to save context $contextId in DAO actor", t)
        failureCallback
      case Failure(t) =>
        logger.error(s"Failed to get context $contextId from DAO", t)
        failureCallback
    }
  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       jobContext: ContextLike,
                       sparkEnv: SparkEnv,
                       existingJobInfo: Option[JobInfo]): Option[Future[Any]] = {
    import akka.util.Timeout

    import scala.concurrent.Await

    def failed(msg: Any): Option[Future[Any]] = {
      sender ! msg
      postEachJob()
      None
    }

    val daoAskTimeout = Timeout(3 seconds)
    // TODO: refactor so we don't need Await, instead flatmap into more futures
    val resp = Await.result(
      (daoActor ? JobDAOActor.GetLastUploadTimeAndType(appName))(daoAskTimeout).
        mapTo[JobDAOActor.LastUploadTimeAndType],
      daoAskTimeout.duration)

    val lastUploadTimeAndType = resp.uploadTimeAndType
    if (lastUploadTimeAndType.isEmpty) return failed(NoSuchApplication)
    val (lastUploadTime, binaryType) = lastUploadTimeAndType.get

    val (jobId, startDateTime) = existingJobInfo match {
      case Some(info) =>
        logger.info(s"Restarting a previously terminated job with id ${info.jobId}" +
            s" and context ${info.contextName}")
        (info.jobId, info.startTime)
      case None =>
        logger.info(s"Creating new JobId for current job")
        (java.util.UUID.randomUUID().toString, DateTime.now())
    }

    val jobContainer = factory.loadAndValidateJob(appName, lastUploadTime,
                                                  classPath, jobCache) match {
      case Good(container) => container
      case Bad(JobClassNotFound) => return failed(NoSuchClass)
      case Bad(JobWrongType) => return failed(WrongJobType)
      case Bad(JobLoadError(ex)) => return failed(JobLoadingError(ex))
    }

    // Automatically subscribe the sender to events so it starts getting them right away
    resultActor ! Subscribe(jobId, sender, events)
    statusActor ! Subscribe(jobId, sender, events)

    val binInfo = BinaryInfo(appName, binaryType, lastUploadTime)
    val jobInfo = JobInfo(jobId, contextId, contextName, binInfo, classPath,
        JobStatus.Running, startDateTime, None, None)

    Some(getJobFuture(jobContainer, jobInfo, jobConfig, sender, jobContext, sparkEnv))
  }

  private def getJobFuture(container: JobContainer,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           jobContext: ContextLike,
                           sparkEnv: SparkEnv): Future[Any] = {

    val jobId = jobInfo.jobId
    logger.info("Starting Spark job {} [{}]...", jobId: Any, jobInfo.classPath)

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, send an error message to the sender, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      sender ! NoJobSlotsAvailable(maxRunningJobs)
      return Future[Any](None)(context.dispatcher)
    }

    Future {
      org.slf4j.MDC.put("jobId", jobId)
      logger.info("Starting job future thread")
      try {
        // Need to re-set the SparkEnv because it's thread-local and the Future runs on a diff thread
        SparkEnv.set(sparkEnv)

        // Use the Spark driver's class loader as it knows about all our jars already
        // NOTE: This may not even be necessary if we set the driver ActorSystem classloader correctly
        Thread.currentThread.setContextClassLoader(jarLoader)
        val job = container.getSparkJob
        try {
          statusActor ! JobStatusActor.JobInit(jobInfo)
          val jobC = jobContext.asInstanceOf[job.C]
          val jobEnv = getEnvironment(jobId)
          job.validate(jobC, jobEnv, jobConfig) match {
            case Bad(reasons) =>
              val err = new Throwable(reasons.toString)
              statusActor ! JobValidationFailed(jobId, DateTime.now(), err)
              throw err
            case Good(jobData) =>
              statusActor ! JobStarted(jobId: String, jobInfo)
              val sc = jobContext.sparkContext
              sc.setJobGroup(jobId, s"Job group for $jobId and spark context ${sc.applicationId}", interruptOnCancel = true)
              job.runJob(jobC, jobEnv, jobData)
          }
        } finally {
          org.slf4j.MDC.remove("jobId")
        }
      } catch {
        case e: java.lang.AbstractMethodError =>
          logger.error("Oops, there's an AbstractMethodError... maybe you compiled " +
            "your code with an older version of SJS? here's the exception:", e)
          throw e
        case e: Throwable =>
          logger.error("Got Throwable", e)
          throw e
      }
    }(executionContext).andThen {
      case Success(result: Any) =>
        // TODO: If the result is Stream[_] and this is running with context-per-jvm=true configuration
        // serializing a Stream[_] blob across process boundaries is not desirable.
        // In that scenario an enhancement is required here to chunk stream results back.
        // Something like ChunkedJobResultStart, ChunkJobResultMessage, and ChunkJobResultEnd messages
        // might be a better way to send results back and then on the other side use chunked encoding
        // transfer to send the chunks back. Alternatively the stream could be persisted here to HDFS
        // and the streamed out of InputStream on the other side.
        // Either way an enhancement would be required here to make Stream[_] responses work
        // with context-per-jvm=true configuration
        resultActor ! JobResult(jobId, result)
        statusActor ! JobFinished(jobId, DateTime.now())
      case Failure(error: Throwable) =>
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        statusActor ! JobErroredOut(jobId, DateTime.now(), error)
        logger.error("Exception from job " + jobId + ": ", error)
    }(executionContext).andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.
        currentRunningJobs.getAndDecrement()
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        postEachJob()
    }(executionContext)
  }

  protected def sendStartJobMessage(receiverActor: ActorRef, msg: StartJob) {
    receiverActor ! msg
  }

  /**
   * During restart scenario, we use best effort approach. So, we try our best to start all
   * the jobs in a context. If some jobs fail to start but others succeed then we just report
   * the failure in logs and continue. If all the jobs fail, then we just kill the context JVM.
   */
  protected def handleJobRestartFailure(jobId: String, error: Throwable, msg: StatusMessage) {
    logger.error(error.getMessage, error)
    totalJobsWhichFailedToRestart += 1
    (totalJobsToRestart - totalJobsWhichFailedToRestart) match {
      case 0 =>
        logger.error(s"Restart report -> $totalJobsWhichFailedToRestart failed out of $totalJobsToRestart")
        val error = Some(ContextKillingItselfException("Job(s) restart failed"))
        setCurrentContextState(ContextInfoModifiable(ContextStatus.Error, error), killMyself(), killMyself())
      case _ =>
        logger.warn(s"Job ($jobId) errored out during restart but continuing", error)
    }
  }

  // Use our classloader and a factory to create the SparkContext.  This ensures the SparkContext will use
  // our class loader when it spins off threads, and ensures SparkContext can find the job and dependent jars
  // when doing serialization, for example.
  def getContextFactory: SparkContextFactory = {
    val factoryClassName = contextConfig.getString("context-factory")
    val factoryClass = jarLoader.loadClass(factoryClassName)
    val factory = factoryClass.newInstance.asInstanceOf[SparkContextFactory]
    Thread.currentThread.setContextClassLoader(jarLoader)
    factory
  }

  // This method should be called after each job is succeeded or failed
  private def postEachJob() {
    // Delete myself after each adhoc job
    if (isAdHoc) {
      become(adhocStopReceive)
      self ! StopContextAndShutdown
    }
  }

  // Protocol like "local" is supported in Spark for Jar loading, but not supported in Java.
  // This method helps convert those Spark URI to those supported by Java.
  // "local" URIs means that the jar must be present on each job server node at the path,
  // as well as on every Spark worker node at the path.
  // For the job server, convert the local to a local file: URI since Java URI doesn't understand local:
  private def convertJarUriSparkToJava(jarUri: String): String = {
    val uri = new URI(jarUri)
    uri.getScheme match {
      case "local" => "file://" + uri.getPath
      case _ => jarUri
    }
  }

  // "Side jars" are jars besides the main job jar that are needed for running the job.
  // They are loaded from the context/job config.
  // Each one should be an URL (http, ftp, hdfs, local, or file). local URLs are local files
  // present on every node, whereas file:// will be assumed only present on driver node
  private def getSideJars(config: Config): Seq[String] =
    Try(config.getStringList("dependent-jar-uris").asScala).
     orElse(Try(config.getString("dependent-jar-uris").split(",").toSeq)).getOrElse(Nil)

  /**
   * This function is responsible for restarting terminated jobs. It selects jobs based on the
   * following criteria
   * a) The job is in Running state
   * b) The job is in Restarting state
   * c) The job is async. Sync jobs are normally short lived and user is waiting on
   *    the other side to receive the response immediately. So, restart feature currently
   *    is only for long running jobs. There is no direct way to check if the job was
   *    originally started with async. Therefore the indirect way via the JobConfig, which
   *    contains this option, is used here.
   */
  private def restartTerminatedJobs(contextId: String, senderRef: ActorRef): Unit = {
    (daoActor ? JobDAOActor.GetJobInfosByContextId(
        contextId, Some(Seq(JobStatus.Running, JobStatus.Restarting))))(daoAskTimeout).onComplete {
      case Success(JobDAOActor.JobInfos(Seq())) =>
        logger.info(s"No job found for context $contextName which was terminated unexpectedly." +
            " Not restarting any job.")
      case Success(JobDAOActor.JobInfos(jobInfos)) =>
        logger.info(s"Found jobs for this context $contextId")

        val restartCandidates = getRestartCandidates(jobInfos)
        logger.info(s"Total restart candidates are ${restartCandidates.length}")
        totalJobsToRestart = restartCandidates.length
        restartCandidates.foreach { jobInfo =>
          (daoActor ? JobDAOActor.GetJobConfig(jobInfo.jobId))(daoAskTimeout).onComplete {
            case Success(JobDAOActor.JobConfig(Some(_config))) =>
              restartJob(jobInfo, _config)
            case Success(JobDAOActor.JobConfig(None)) =>
              updateJobInfoAndReportFailure(jobInfo, NoJobConfigFoundException(jobInfo.jobId))
            case Failure(e: Exception) =>
              // In case of error during job restart, an error will be written to the DAO.
              // In case that this job is a dependency for another job, it may cause problems.
              // A strategy must be implemented at this point.
              updateJobInfoAndReportFailure(jobInfo, e)
            case _ =>
              updateJobInfoAndReportFailure(jobInfo, UnexpectedMessageReceivedException(jobInfo.jobId))
          }
        }
      case Failure(e: Exception) =>
        logger.error(s"Exception occured while accessing job ids for context $contextId from DAO.", e)
        // This failure might be temporary, try to update context if possible
        val error = Some(ContextKillingItselfException(s"Failed to fetch jobs for context $contextId"))
        setCurrentContextState(ContextInfoModifiable(ContextStatus.Error, error), killMyself(), killMyself())
      case unexpectedMsg @ _ =>
        logger.error(s"Unexpected scenario occured, message received is $unexpectedMsg")
        self ! PoisonPill
    }
  }

  private def updateJobInfoAndReportFailure(jobInfo: JobInfo, error: Exception) {
    updateJobInfoWithErrorState(jobInfo, error)
    self ! JobRestartFailed(jobInfo.jobId, error)
  }

  private def updateJobInfoWithErrorState(jobInfo: JobInfo, error: Throwable) {
   val updatedJobInfo = jobInfo.copy(state = JobStatus.Error,
     endTime = Some(DateTime.now()), error = Some(ErrorData(error)))
   daoActor ! JobDAOActor.SaveJobInfo(updatedJobInfo)
  }

  private def restartJob(existingJobInfo: JobInfo, existingJobConfig: Config) {
    // Add response to master for testing
    var respMsg = s"Restarting the last job (JobId=${existingJobInfo.jobId} & "
    respMsg = respMsg + s"contextName=${existingJobInfo.contextName})"
    logger.info(respMsg)
    val fullJobConfig = existingJobConfig.withFallback(config).resolve()
    val events: Set[Class[_]] = Set(classOf[JobStarted]) ++ Set(classOf[JobValidationFailed])

    sendStartJobMessage(self, StartJob(existingJobInfo.binaryInfo.appName,
        existingJobInfo.classPath, fullJobConfig, events, Some(existingJobInfo)))
    logger.info(s"Job restart message has been sent for old job (${existingJobInfo.jobId})" +
        s" and context ${existingJobInfo.contextName}.")
  }

  private def getRestartCandidates(jobInfos: Seq[JobInfo]): Seq[JobInfo] = {
    jobInfos.filter { jobInfo =>
      jobInfo.state match {
        case JobStatus.Running | JobStatus.Restarting => true
        case _ => false
      }
    }
  }

  @VisibleForTesting
  protected def scheduleContextStopTimeoutMsg(sender: ActorRef): Option[Cancellable] = {
    val stopTimeoutMsg = ContextStopScheduledMsgTimeout(sender)
    contextStopTimeoutMsgHelper(stopTimeoutMsg)
  }

  private def scheduleContextStopForcefullyTimeoutMsg(sender: ActorRef): Option[Cancellable] = {
    val stopTimeoutMsg = ContextStopForcefullyScheduledMsgTimeout(sender)
    contextStopTimeoutMsgHelper(stopTimeoutMsg)
  }

  private def contextStopTimeoutMsgHelper(stopTimeoutMsg: ContextStopSchedule):
      Option[Cancellable] = {
    // Timeout is (contextDeletionTimeout - 2) because we want to give some time to this
    // actor to process the timeout message and send a response back.
    val msgTimeoutSeconds = (contextDeletionTimeout - 2).seconds
    Try(context.system.scheduler.scheduleOnce(msgTimeoutSeconds, self, stopTimeoutMsg)) match {
      case Success(timeoutMsgHandler) =>
        logger.info("Scheduled a time out message for forceful context stop")
        Some(timeoutMsgHandler)
      case Failure(e) =>
        logger.error("Failed to schedule a time out message for forceful context stop", e)
        None
    }
  }

  private def stopContextForcefullyHelper(originalSender: ActorRef): Unit = {
    logger.info("Shutting down forcefully SparkContext {}", contextName)
    Option(jobContext) match {
      case Some(_context) =>
        val cancelHandler = scheduleContextStopForcefullyTimeoutMsg(originalSender)
        stopContextSenderAndHandler = (Some(originalSender), cancelHandler)
        val appId = jobContext.sparkContext.applicationId
        val forcefulKill = new StandaloneForcefulKill(config, appId)
        try {
          forcefulKillCaller(forcefulKill)
        } catch {
          case e: Exception =>
            cancelHandler match {
              case Some(handler) => handler.cancel()
              case None => //in this case nothing has to be done
            }
            stopContextSenderAndHandler = (None, None)
            become(stoppingStateReceive)
            originalSender ! ContextStopError(e)
        }
      case None =>
        logger.warn("Context was null, killing myself")
        originalSender ! SparkContextStopped
        self ! PoisonPill
    }
  }

  @VisibleForTesting
  protected def forcefulKillCaller(forcefulKill: StandaloneForcefulKill): Unit = {
    forcefulKill.kill()
  }
}
