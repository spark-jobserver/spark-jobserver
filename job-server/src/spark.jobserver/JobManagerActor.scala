package spark.jobserver

import java.util.concurrent.Executors._
import akka.actor.{ActorRef, Props, PoisonPill}
import com.typesafe.config.Config
import java.net.{URI, URL}
import java.util.concurrent.atomic.AtomicInteger
import ooyala.common.akka.InstrumentedActor
import org.apache.spark.{ SparkEnv, SparkContext }
import org.joda.time.DateTime
import scala.collection.mutable
import scala.concurrent.{ Future, ExecutionContext }
import scala.util.{Failure, Success, Try}
import spark.jobserver.ContextSupervisor.StopContext
import spark.jobserver.io.{JobDAOActor, JobDAO, JobInfo, JarInfo}
import spark.jobserver.util.{ContextURLClassLoader, SparkJobUtils}
//每个上下文进来，都要创建 JobManagerActor 对象
object JobManagerActor {
  // Messages
  case class Initialize(daoActor: ActorRef, resultActorOpt: Option[ActorRef])
  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]])
  case class KillJob(jobId: String)
  case object SparkContextStatus
  //记录当前有多少条sql在运行
  case object SparkContextSqlNum
  case class ContextContainSqlNum(contextName: String, sqlNum: Int, maxRunningJobs :Int)
//显示出当前的context中的信息
  case object SparkContextInfo
  case class ContextInfoMsg(contextName: String, sqlNum: Int ,maxRunningJobs :Int ,applicationId:String)
  // Results/Data
  case class Initialized(contextName: String, resultActor: ActorRef)
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)
  case object SparkContextAlive
  case object SparkContextDead

  // Akka 2.2.x style actor props for actor creation
  def props(contextConfig: Config): Props = Props(classOf[JobManagerActor], contextConfig)
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
class JobManagerActor(contextConfig: Config) extends InstrumentedActor {

  import CommonMessages._
  import JobManagerActor._
  import scala.util.control.Breaks._
  import collection.JavaConverters._

  val config = context.system.settings.config
  private val maxRunningJobs = SparkJobUtils.getMaxRunningJobs(config)
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxRunningJobs))

  var jobContext: ContextLike = _
  var sparkEnv: SparkEnv = _
  protected var rddManagerActor: ActorRef = _

  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.

  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  private val jobCacheEnabled = Try(config.getBoolean("spark.job-cache.enabled")).getOrElse(false)
  // Use Spark Context's built in classloader when SPARK-1230 is merged.
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)
  //这个就是上下文名称
  private val contextName = contextConfig.getString("context.name")
  private val isAdHoc = Try(contextConfig.getBoolean("is-adhoc")).getOrElse(false)

  //NOTE: Must be initialized after sparkContext is created
  private var jobCache: JobCache = _

  private var statusActor: ActorRef = _
  protected var resultActor: ActorRef = _
  private var daoActor: ActorRef = _


  override def postStop() {
    logger.info("Shutting down SparkContext {}", contextName)
    Option(jobContext).foreach(_.stop())
  }

  /**
    * 这里接收 job的任务动作，如果是多进程，其实是运行于spark 的那个进程当中
    * @return
    */
  def wrappedReceive: Receive = {
    case Initialize(dao, resOpt) =>
      //在这里进行初始化上下文了
      daoActor = dao
      statusActor = context.actorOf(JobStatusActor.props(daoActor))
      resultActor = resOpt.getOrElse(context.actorOf(Props[JobResultActor]))

      try {
        // Load side jars first in case the ContextFactory comes from it
        getSideJars(contextConfig).foreach { jarUri =>
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
        }
        //动态上下文就是在这里了
        jobContext = createContextFromConfig()
        sparkEnv = SparkEnv.get
        rddManagerActor = context.actorOf(Props(classOf[RddManagerActor], jobContext.sparkContext),
                                          "rdd-manager-actor")
        jobCache = new JobCache(jobCacheSize, daoActor, jobContext.sparkContext, jarLoader)
        getSideJars(contextConfig).foreach { jarUri => jobContext.sparkContext.addJar(jarUri) }
        sender ! Initialized(contextName, resultActor)
      } catch {
        case t: Throwable =>
          logger.error("Failed to create context " + contextName + ", shutting down actor", t)
          sender ! InitError(t)
          self ! PoisonPill
      }

    case StartJob(appName, classPath, jobConfig, events) =>
      startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv, rddManagerActor)

    case KillJob(jobId: String) => {
      //开始kill job了
      jobContext.sparkContext.cancelJobGroup(jobId)
      logger.info("stop spark  job id {}",jobId)
      //其实这里未必真正能kill的掉的
      statusActor ! JobKilled(jobId, DateTime.now())
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
            logger.error("SparkContext is not exist!")
            sender ! SparkContextDead
          }
        }
      }
    }

    case SparkContextSqlNum => {
        sender ! ContextContainSqlNum(contextName , currentRunningJobs.get() ,maxRunningJobs)
    }
    case SparkContextInfo => {
      sender ! ContextInfoMsg(contextName , currentRunningJobs.get() ,maxRunningJobs ,
        jobContext.sparkContext.applicationId)
    }

  }

  /**
    * 这里就是入口了
    * @param appName
    * @param classPath
    * @param jobConfig
    * @param events
    * @param jobContext
    * @param sparkEnv
    * @param rddManagerActor
    * @return
    */
  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       jobContext: ContextLike,
                       sparkEnv: SparkEnv,
                       rddManagerActor: ActorRef): Option[Future[Any]] = {
    var future: Option[Future[Any]] = None
    breakable {
      import akka.pattern.ask
      import akka.util.Timeout
      import scala.concurrent.duration._
      import scala.concurrent.Await

      val daoAskTimeout = Timeout(3 seconds)
      // TODO: refactor so we don't need Await, instead flatmap into more futures
      val resp = Await.result(
        (daoActor ? JobDAOActor.GetLastUploadTime(appName))(daoAskTimeout).mapTo[JobDAOActor.LastUploadTime],
        daoAskTimeout.duration)

      val lastUploadTime = resp.lastUploadTime
      if (!lastUploadTime.isDefined) {
        //通过Actor模型进行发送信息
        sender ! NoSuchApplication
        postEachJob()
        break
      }

      // Check appName, classPath from jar
      val jarInfo = JarInfo(appName, lastUploadTime.get)
      val jobId = java.util.UUID.randomUUID().toString()
    //  logger.info("Loading class {} for app {}", classPath, appName: Any)
      val jobJarInfo = try {
        jobCache.getSparkJob(jarInfo.appName, jarInfo.uploadTime, classPath)
      } catch {
        case _: ClassNotFoundException =>
          sender ! NoSuchClass
          postEachJob()
          break
          null // needed for inferring type of return value
        case err: Throwable =>
          sender ! JobLoadingError(err)
          postEachJob()
          break
          null
      }

      // Validate that job fits the type of context we launched
      //通过Actor模型进行发送信息，这里创建对象，并且validate
      val job = jobJarInfo.constructor()
      if (!jobContext.isValidJob(job)) {
        sender ! WrongJobType
        break
      }

      // Automatically subscribe the sender to events so it starts getting them right away
      //定阅消息
      resultActor ! Subscribe(jobId, sender, events)
      statusActor ! Subscribe(jobId, sender, events)

      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, DateTime.now(), None, None,None)
      //开始跑了
      future =
        Option(getJobFuture(jobJarInfo, jobInfo, jobConfig, sender, jobContext, sparkEnv,
                            rddManagerActor))
    }

    future
  }

  /**
    * 这里就是直接运行job的地方
    * @param jobJarInfo
    * @param jobInfo
    * @param jobConfig
    * @param subscriber
    * @param jobContext
    * @param sparkEnv
    * @param rddManagerActor
    * @return
    */
  private def getJobFuture(jobJarInfo: JobJarInfo,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           jobContext: ContextLike,
                           sparkEnv: SparkEnv,
                           rddManagerActor: ActorRef): Future[Any] = {

    val jobId = jobInfo.jobId
    val constructor = jobJarInfo.constructor
    //开始跑了
   // logger.info("Starting Spark job {} [{}]...", jobId: Any, jobJarInfo.className)

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, send an error message to the sender, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      //发送消息，现在太多job在跑了，失败
      sender ! NoJobSlotsAvailable(maxRunningJobs)
      return Future[Any](None)(context.dispatcher)
    }

    Future {
      org.slf4j.MDC.put("jobId", jobId)
      logger.info("Starting job future thread")

      // Need to re-set the SparkEnv because it's thread-local and the Future runs on a diff thread
      SparkEnv.set(sparkEnv)

      // Use the Spark driver's class loader as it knows about all our jars already
      // NOTE: This may not even be necessary if we set the driver ActorSystem classloader correctly
      Thread.currentThread.setContextClassLoader(jarLoader)
      val job = constructor()
      if (job.isInstanceOf[NamedRddSupport]) {
        //如果是可复用rdd的job
        val namedRdds = job.asInstanceOf[NamedRddSupport].namedRddsPrivate
        if (namedRdds.get() == null) {
          namedRdds.compareAndSet(null, new JobServerNamedRdds(rddManagerActor))
        }
      }

      try {

        val jobC = jobContext.asInstanceOf[job.C]
        //发送job初始化
        statusActor ! JobStatusActor.JobInit(jobInfo)


        //这里进行了validate
        job.validate(jobC, jobConfig) match {
          case SparkJobInvalid(reason) => {
            val err = new Throwable(reason)
            statusActor ! JobValidationFailed(jobId, DateTime.now(), err)
            throw err
          }
          case SparkJobValid => {
            statusActor ! JobStarted(jobId: String, contextName, jobInfo.startTime)
            val sc = jobContext.sparkContext
            sc.setJobGroup(jobId, s"Job group for $jobId and spark context ${sc.applicationId}", true)
            logger.info(String.format("start a new job %s in application %s ",jobId,sc.applicationId))
            try {
              //这里真正开始跑了
             job.runJob(jobC, jobConfig)
            } catch {
              case e: Throwable =>
                logger.error("run jon error msg:" + e.getMessage,e)
                val err = new Throwable(e.getMessage ,e)
                throw err
            }
          }
        }
      } finally {
        org.slf4j.MDC.remove("jobId")
      }
    }(executionContext).andThen {
      case Success(result: Any) =>
        statusActor ! JobFinished(jobId, DateTime.now())
        resultActor ! JobResult(jobId, result)
      case Failure(error: Throwable) =>
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        statusActor ! JobErroredOut(jobId, DateTime.now(), error)
        logger.warn("Exception from job " + jobId + ": ", error)

    }(executionContext).andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.


        logger.info("decrement count ,the job {} finish",jobId)
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        currentRunningJobs.getAndDecrement()
        postEachJob()
    }(executionContext)
  }

  // Use our classloader and a factory to create the SparkContext.  This ensures the SparkContext will use
  // our class loader when it spins off threads, and ensures SparkContext can find the job and dependent jars
  // when doing serialization, for example.
  def createContextFromConfig(contextName: String = contextName): ContextLike = {
    val factoryClassName = contextConfig.getString("context-factory")
    val factoryClass = jarLoader.loadClass(factoryClassName)
    val factory = factoryClass.newInstance.asInstanceOf[spark.jobserver.context.SparkContextFactory]
    Thread.currentThread.setContextClassLoader(jarLoader)
    factory.makeContext(config, contextConfig, contextName)
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
