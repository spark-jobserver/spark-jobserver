package spark.jobserver

import java.net.{URI, URL}
import java.util.concurrent.Executors._
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, PoisonPill, Props}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkEnv}
import org.joda.time.DateTime
import org.scalactic._
import spark.jobserver.api.JobEnvironment
import spark.jobserver.context.{JobContainer, SparkContextFactory}
import spark.jobserver.io.{BinaryInfo, JobDAOActor, JobInfo}
import spark.jobserver.util.{ContextURLClassLoader, SparkJobUtils}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

object JobManagerActor {
  // Messages
  case class Initialize(resultActorOpt: Option[ActorRef])
  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]])
  case class KillJob(jobId: String)
  case class JobKilledException(jobId: String) extends Exception(s"Job $jobId killed")

  case object GetContextConfig
  case object SparkContextStatus

  // Results/Data
  case class ContextConfig(contextName: String, contextConfig: SparkConf, hadoopConfig: Configuration)
  case class Initialized(contextName: String, resultActor: ActorRef)
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)
  case object SparkContextAlive
  case object SparkContextDead

  // Akka 2.2.x style actor props for actor creation
  def props(contextConfig: Config, daoActor: ActorRef): Props = Props(classOf[JobManagerActor],
    contextConfig, daoActor)
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
class JobManagerActor(contextConfig: Config, daoActor: ActorRef) extends InstrumentedActor {

  import CommonMessages._
  import JobManagerActor._

  import collection.JavaConverters._

  val config = context.system.settings.config
  private val maxRunningJobs = SparkJobUtils.getMaxRunningJobs(config)
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxRunningJobs))

  var jobContext: ContextLike = _
  var sparkEnv: SparkEnv = _

  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.

  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  private val jobCacheEnabled = Try(config.getBoolean("spark.job-cache.enabled")).getOrElse(false)
  // Use Spark Context's built in classloader when SPARK-1230 is merged.
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)
  private val contextName = contextConfig.getString("context.name")
  private val isAdHoc = Try(contextConfig.getBoolean("is-adhoc")).getOrElse(false)

  //NOTE: Must be initialized after sparkContext is created
  private var jobCache: JobCache = _

  private var statusActor: ActorRef = _
  protected var resultActor: ActorRef = _
  private var factory: SparkContextFactory = _

  private val jobServerNamedObjects = new JobServerNamedObjects(context.system)

  private def getEnvironment(_jobId: String): JobEnvironment = {
    val _contextCfg = contextConfig
    new JobEnvironment {
      def jobId: String = _jobId
      def namedObjects: NamedObjects = jobServerNamedObjects
      def contextConfig: Config = _contextCfg
    }
  }

  override def postStop() {
    logger.info("Shutting down SparkContext {}", contextName)
    Option(jobContext).foreach(_.stop())
  }

  def wrappedReceive: Receive = {
    case Initialize(resOpt) =>
      statusActor = context.actorOf(JobStatusActor.props(daoActor))
      resultActor = resOpt.getOrElse(context.actorOf(Props[JobResultActor]))

      try {
        // Load side jars first in case the ContextFactory comes from it
        getSideJars(contextConfig).foreach { jarUri =>
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
        }
        factory = getContextFactory()
        jobContext = factory.makeContext(config, contextConfig, contextName)
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

    case StartJob(appName, classPath, jobConfig, events) => {
      val loadedJars = jarLoader.getURLs
      getSideJars(jobConfig).foreach { jarUri =>
        val jarToLoad = new URL(convertJarUriSparkToJava(jarUri))
        if(! loadedJars.contains(jarToLoad)){
          logger.info("Adding {} to Current Job Class path", jarUri)
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
          jobContext.sparkContext.addJar(jarUri)
        }
      }
      startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv)
    }

    case KillJob(jobId: String) => {
      jobContext.sparkContext.cancelJobGroup(jobId)
      val resp = JobKilled(jobId, DateTime.now())
      statusActor ! resp
      sender ! resp
    }

    case SparkContextStatus => {
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          jobContext.sparkContext.getSchedulingMode
          sender ! SparkContextAlive
        } catch {
          case e: Exception => {
            logger.error("SparkContext does not exist!")
            sender ! SparkContextDead
          }
        }
      }
    }
    case GetContextConfig => {
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          val conf: SparkConf = jobContext.sparkContext.getConf
          val hadoopConf: Configuration = jobContext.sparkContext.hadoopConfiguration
          sender ! ContextConfig(jobContext.sparkContext.appName, conf, hadoopConf)
        } catch {
          case e: Exception => {
            logger.error("SparkContext does not exist!")
            sender ! SparkContextDead
          }
        }
      }
    }
  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       jobContext: ContextLike,
                       sparkEnv: SparkEnv): Option[Future[Any]] = {
    import akka.pattern.ask
    import akka.util.Timeout
    import spark.jobserver.context._

    import scala.concurrent.Await
    import scala.concurrent.duration._

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
    if (!lastUploadTimeAndType.isDefined) return failed(NoSuchApplication)
    val (lastUploadTime, binaryType) = lastUploadTimeAndType.get

    val jobId = java.util.UUID.randomUUID().toString()
    val jobContainer = factory.loadAndValidateJob(appName, lastUploadTime,
                                                  classPath, jobCache) match {
      case Good(container)       => container
      case Bad(JobClassNotFound) => return failed(NoSuchClass)
      case Bad(JobWrongType)     => return failed(WrongJobType)
      case Bad(JobLoadError(ex)) => return failed(JobLoadingError(ex))
    }

    // Automatically subscribe the sender to events so it starts getting them right away
    resultActor ! Subscribe(jobId, sender, events)
    statusActor ! Subscribe(jobId, sender, events)

    val binInfo = BinaryInfo(appName, binaryType, lastUploadTime)
    val jobInfo = JobInfo(jobId, contextName, binInfo, classPath, DateTime.now(), None, None)

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
              sc.setJobGroup(jobId, s"Job group for $jobId and spark context ${sc.applicationId}", true)
              job.runJob(jobC, jobEnv, jobData)
          }
        } finally {
          org.slf4j.MDC.remove("jobId")
        }
      } catch {
        case e: java.lang.AbstractMethodError => {
          logger.error("Oops, there's an AbstractMethodError... maybe you compiled " +
            "your code with an older version of SJS? here's the exception:", e)
          throw e
        }
        case e: Throwable => {
          logger.error("Got Throwable", e)
          throw e
        };
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
        // Wrapping the error inside a RuntimeException to handle the case of throwing custom exceptions.
        val wrappedError = wrapInRuntimeException(error)
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        statusActor ! JobErroredOut(jobId, DateTime.now(), wrappedError)
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

  // Wraps a Throwable object into a RuntimeException. This is useful in case
  // a custom exception is thrown. Currently, throwing a custom exception doesn't
  // work and this is a workaround to wrap it into a standard exception.
  protected def wrapInRuntimeException(t: Throwable): RuntimeException = {
    val cause : Throwable = getRootCause(t)
    val e : RuntimeException = new RuntimeException("%s: %s"
      .format(cause.getClass().getName() ,cause.getMessage))
    e.setStackTrace(cause.getStackTrace())
    return e
  }

  // Gets the very first exception that caused the current exception to be thrown.
  protected def getRootCause(t: Throwable): Throwable = {
    var result : Throwable = t
    var cause : Throwable = result.getCause()
    while(cause != null  && (result != cause) ) {
      result = cause
      cause = result.getCause()
    }
    return result
  }

  // Use our classloader and a factory to create the SparkContext.  This ensures the SparkContext will use
  // our class loader when it spins off threads, and ensures SparkContext can find the job and dependent jars
  // when doing serialization, for example.
  def getContextFactory(): SparkContextFactory = {
    val factoryClassName = contextConfig.getString("context-factory")
    val factoryClass = jarLoader.loadClass(factoryClassName)
    val factory = factoryClass.newInstance.asInstanceOf[SparkContextFactory]
    Thread.currentThread.setContextClassLoader(jarLoader)
    factory
  }

  // This method should be called after each job is succeeded or failed
  private def postEachJob() {
    // Delete myself after each adhoc job
    if (isAdHoc) self ! PoisonPill
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
    Try(config.getStringList("dependent-jar-uris").asScala.toSeq).
     orElse(Try(config.getString("dependent-jar-uris").split(",").toSeq)).getOrElse(Nil)
}
