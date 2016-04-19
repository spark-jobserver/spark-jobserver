package spark.jobserver

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success, Try}
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberUp, MemberEvent, InitialStateAsEvents}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.JobManagerActor.SparkContextSqlNum
import spark.jobserver.util.SparkJobUtils
import scala.collection.mutable
import scala.util.{Try, Success, Failure}
import scala.sys.process._


import akka.actor.{Terminated, Props, ActorRef, PoisonPill}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.JobManagerActor._
import spark.jobserver.io.JobDAO
import spark.jobserver.util.SparkJobUtils
import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

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
  val contextTimeout = SparkJobUtils.getContextTimeout(config)
  val managerStartCommand = config.getString("deploy.manager-start-cmd")
  val dropOldContextCommand = config.getString("deploy.drop-old-context-cmd")

  import context.dispatcher

  //actor name -> (context isadhoc, success callback, failure callback)
  //TODO: try to pass this state to the jobManager at start instead of having to track
  //extra state.  What happens if the WebApi process dies before the forked process
  //starts up?  Then it never gets initialized, and this state disappears.
  private val contextInitInfos = mutable.HashMap.empty[String,
    (Boolean, (ActorRef,String) => Unit, Throwable => Unit,String)]

  // actor name -> (JobManagerActor ref, ResultActor ref)
  private val contexts = mutable.HashMap.empty[String, (ActorRef, ActorRef,String)]
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
  private object Locker
  def wrappedReceive: Receive = {
    case MemberUp(member) =>
      if (member.hasRole("manager")) {
        val memberActors = RootActorPath(member.address) / "user" / "*"
        context.actorSelection(memberActors) ! Identify(memberActors)
      }

    case ActorIdentity(memberActors, actorRefOpt) =>
      actorRefOpt.map { actorRef =>
        val actorName = actorRef.path.name
        //应该是通过sh进程拉起的子进程，已经主动连接回来这里了
        if (actorName.startsWith("jobManager")) {
          logger.info("Received identify response, attempting to initialize context at {}", memberActors)
          (for { (isAdHoc, successFunc, failureFunc ,contextProcessId) <- contextInitInfos.remove(actorName) }
           yield {
             initContext(actorName, actorRef, contextInitTimeout,contextProcessId )(isAdHoc,
               successFunc, failureFunc)
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

    case ListContextsContainSqlNum  =>
      //返回context里面包含多少条运行的Sql
      var contextsSql = mutable.HashMap.empty[String,( String ,Int ,Int)]
      val keynum = contexts.keys.size
      if(keynum == 0 ){
        sender ! contextsSql.values.toSeq

      } else {
        var  othermsg:Int = 0;
        for (   name <- contexts.keys  ) {
          val originator = sender
          val future = (contexts(name)._1 ? SparkContextSqlNum) (contextTimeout.seconds)
          future collect {
            case JobManagerActor.ContextContainSqlNum(contextName, sqlNum, maxRunningJobs) =>
              Locker.synchronized {
                contextsSql(name) = (name, sqlNum, maxRunningJobs)
                if (keynum == (contextsSql.toSeq.length + othermsg)) {
                  //logger.info("send all length in it  {}  ", contextsSql.toSeq.length)
                  originator ! contextsSql.values.toSeq
                }
              }
            case _ =>
              logger.warn("when send  SparkContextInfo ,it get other messag")
              othermsg = othermsg + 1
              if (keynum == (contextsSql.toSeq.length + othermsg)) {
                //logger.info("send all length in it  {}  ", contextsSql.toSeq.length)
                originator ! contextsSql.values.toSeq
              }
          }
        }
      }

    case ListContextsInfo  =>
      //返回context里面包含所有的context的信息
      var contextsInfo = mutable.HashMap.empty[String,( String ,Int ,Int,String)]
      val keynum = contexts.keys.size
      if(keynum == 0 ){
        sender ! contextsInfo.values.toSeq

      } else {
        var othermsg: Int = 0;
        for (name <- contexts.keys) {
          val originator = sender
          val future = (contexts(name)._1 ? SparkContextInfo) (contextTimeout.seconds)
          future collect {
            case JobManagerActor.ContextInfoMsg(contextName, sqlNum, maxRunningJobs, applicationId) =>
              Locker.synchronized {
                contextsInfo(name) = (name, sqlNum, maxRunningJobs, applicationId)
                if (keynum == (contextsInfo.toSeq.length + othermsg)) {
                  logger.info("send all length in it  {}  ", contextsInfo.toSeq.length)
                  originator ! contextsInfo.values.toSeq
                }
              }
            case _ =>
              logger.warn("when send  SparkContextInfo ,it get other messag")
              othermsg = othermsg + 1
              if (keynum == (contextsInfo.toSeq.length + othermsg)) {
                logger.info("send all length in it  {}  ", contextsInfo.toSeq.length)
                originator ! contextsInfo.values.toSeq
              }
          }
        }
      }



    case AddContext(name, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      // TODO(velvia): This check is not atomic because contexts is only populated
      // after SparkContext successfully created!  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349
      if (contexts contains name) {
        //已经有这人上下文了
        originator ! ContextAlreadyExists
      } else {
        //这里起动上下文，同时起动脚本进行处理了
        startContext(name, mergedConfig, false) { (ref,temLogPathName) =>
          originator ! ContextInitialized(temLogPathName)
        } { err =>
          originator ! ContextInitError(err)
        }
      }


    case DropOldContext(contextProcessId) =>
      val originator = sender()

        //这里清理可能的上下文，同时起动脚本进行处理了
      dropOldContextProcess(contextProcessId ) { ( result) =>
          originator ! DropContextSuccs(result)
        } { err =>
          originator ! DropContextError(err)
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
  //这里起动上下文，同时起动脚本进行处理了
      startContext(contextName, mergedConfig, true) { (ref,contextProcessId) =>
        originator ! contexts(contextName)
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! contexts.get(name).map(_._2).getOrElse(globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        sender ! contexts(name)
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info("Shutting down context {}", name)
        //这里发个命令过去，就停止进程了
        contexts(name)._1 ! PoisonPill
        sender ! ContextStopped

        //有可能旧的进程没有清理，这里调用一个脚本清理一下拉起来的进程
        dropOldContextProcess(   contexts(name)._3 ) { ( result) =>
        } { err =>  }


      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      //
      logger.info("Actor terminated: {}", name)
      contexts.retain { case (name, (jobMgr, resActor,contextProcessId)) => jobMgr != actorRef }
  }

  /**
    * 最后整个contex还要这里进行初始化一下
    * 由于这里是多进程的，这里还是父进程的创建环节
    * @param actorName
    * @param ref
    * @param timeoutSecs
    * @param isAdHoc
    * @param successFunc
    * @param failureFunc
    */
  private def initContext(actorName: String, ref: ActorRef, timeoutSecs: Long = 1 ,contextProcessId:String)
                         (isAdHoc: Boolean,
                          successFunc: (ActorRef,String) => Unit,
                          failureFunc: Throwable => Unit): Unit = {
    import akka.pattern.ask

    val resultActor = if (isAdHoc) globalResultActor else context.actorOf(Props(classOf[JobResultActor]))
    (ref ? JobManagerActor.Initialize(
      daoActor, Some(resultActor)))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e:Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        ref ! PoisonPill
        failureFunc(e)
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        ref ! PoisonPill
        failureFunc(t)
      case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        contexts(ctxName) = (ref, resActor,contextProcessId)
        context.watch(ref)
        successFunc(ref,contextProcessId)
    }
  }
//整个job的起动就是在这里了
  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean)
                          (successFunc: (ActorRef ,String)=> Unit)(failureFunc: Throwable => Unit): Unit = {
    require(!(contexts contains name), "There is already a context named " + name)
    val contextActorName = "jobManager-" + java.util.UUID.randomUUID().toString.substring(16)

    logger.info("Starting context with actor name {}", contextActorName)

    //创建上下文目录
    val contextDir: java.io.File = try {
        createContextDir(name, contextConfig, isAdHoc, contextActorName)
      } catch {
        case e: Exception =>
          failureFunc(e)
          return
      }
    val contextProcessId = contextDir.getName
    //这里拉起一个进程进行处理了
    val pb = Process(s"$managerStartCommand $contextDir ${selfAddress.toString}")
    val pio = new ProcessIO(_ => (),
                        stdout => scala.io.Source.fromInputStream(stdout)
                          .getLines.foreach(println),
                        stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(println))
    logger.info("Starting to execute sub process {}", pb)
    val processStart = Try {
      //这里拉起一个进程
      val process = pb.run(pio)
      val exitVal = process.exitValue()
      if (exitVal != 0) {
        throw new IOException("Failed to launch context process, got exit code " + exitVal)
      }
    }

    if (processStart.isSuccess) {
      contextInitInfos(contextActorName) = (isAdHoc, successFunc, failureFunc ,contextProcessId)
    } else {
      failureFunc(processStart.failed.get)
    }

  }


  /**
    * 删除旧的context进程
    * 有可能会失败，因为进程本来可能就不存在的
    * @param contextProcessId
    */
  private def dropOldContextProcess(contextProcessId: String )
                            (successFunc:  String => Unit)(failureFunc: Throwable => Unit): Unit = {

    //这里拉起一个进程进行处理了
    val pb = Process(s"$dropOldContextCommand $contextProcessId  ")
    val pio = new ProcessIO(_ => (),
      stdout => scala.io.Source.fromInputStream(stdout)
        .getLines.foreach(println),
      stderr => scala.io.Source.fromInputStream(stderr).getLines().foreach(println))
    logger.info("Starting to execute sub process {}", pb)
    val processStart = Try {
      //这里拉起一个进程
      val process = pb.run(pio)
      val exitVal = process.exitValue()
      if (exitVal != 0) {
        //不用返回，因为可能进程本来就不存在的
        throw new IOException("Failed to drop context process, got exit code " + exitVal)
      }
    }

    if (processStart.isSuccess) {
      logger.debug(s" drop success old context process $contextProcessId")
      successFunc(s" drop success old context process $contextProcessId")
    } else {
      logger.error(s"error   drop context $contextProcessId", processStart.failed.get)
      failureFunc(processStart.failed.get)
    }

  }

  /**
    * 创建这个context 使用的临时目录
    * @param name
    * @param contextConfig
    * @param isAdHoc
    * @param actorName
    * @return
    */
  private def createContextDir(name: String,
                               contextConfig: Config,
                               isAdHoc: Boolean,
                               actorName: String): java.io.File = {
    // Create a temporary dir, preferably in the LOG_DIR
    //创建日志目录
    val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")
    val tmpDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$encodedContextName")
    }.getOrElse(Files.createTempDirectory("jobserver"))
    logger.info("Created working directory {} for context {}", tmpDir: Any, name)

    // Now create the contextConfig merged with the values we need
    val mergedConfig = ConfigFactory.parseMap(
                         Map("is-adhoc" -> isAdHoc.toString,
                             "context.name" -> name,
                             "context.actorname" -> actorName).asJava
                       ).withFallback(contextConfig)

    // Write out the config to the temp dir
    //把配置都写到这个临时目录中去
    Files.write(tmpDir.resolve("context.conf"),
                Seq(mergedConfig.root.render(ConfigRenderOptions.concise)).asJava,
                Charset.forName("UTF-8"))

    tmpDir.toFile
  }

  private def addContextsFromConfig(config: Config) {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, false) {( ref,temLogPathName) => } {
          e => logger.error("Unable to start context" + contextName, e)
        }
      }
    }

  }
}
