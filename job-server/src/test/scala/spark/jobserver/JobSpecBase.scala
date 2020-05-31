package spark.jobserver

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.context.DefaultSparkContextFactory
import spark.jobserver.io.JobDAOActor.{GetLastBinaryInfo, LastBinaryInfo, SaveBinary, SaveBinaryResult}
import spark.jobserver.io.{BinaryDAO, BinaryInfo, BinaryType, DAOTestsHelper, JobDAO, MetaDataDAO}
import spark.jobserver.util.JobserverConfig

import scala.concurrent.duration._
import scala.util.Success

/**
 * Provides a base Config for tests.  Override the vals to configure.  Mix into an object.
 * Also, defaults for values not specified here could be provided as java system properties.
 */
trait JobSpecConfig {
  import collection.JavaConverters._

  val JobResultCacheSize = Integer.valueOf(30)
  // number of cores to allocate. Required.
  val NumCpuCores = Integer.valueOf(Runtime.getRuntime.availableProcessors())
  // Executor memory per node, -Xmx style eg 512m, 1G, etc.
  val MemoryPerNode = "512m"
  val MaxJobsPerContext = Integer.valueOf(2)
  def contextFactory: String = classOf[DefaultSparkContextFactory].getName

  lazy val config = {
    val ConfigMap = Map(
      "akka.loglevel" -> "OFF",
      "spark.jobserver.job-result-cache-size" -> JobResultCacheSize,
      "spark.jobserver.dao-timeout" -> "3s",
      "spark.jobserver.context-deletion-timeout" -> "5s",
      "num-cpu-cores" -> NumCpuCores,
      "memory-per-node" -> MemoryPerNode,
      "spark.jobserver.max-jobs-per-context" -> MaxJobsPerContext,
      "spark.jobserver.named-object-creation-timeout" -> "60 s",
      "akka.log-dead-letters" -> Integer.valueOf(0),
      "spark.master" -> "local[*]",
      "spark.driver.host" -> "127.0.0.1",
      "context-factory" -> contextFactory,
      "spark.context-settings.test" -> "",
      "akka.test.single-expect-default" -> "6s",
      "akka.test.timefactor" -> 2,
      "spark.driver.allowMultipleContexts" -> true,
      JobserverConfig.IS_SPARK_SESSION_HIVE_ENABLED -> "true"
    )
    ConfigFactory.parseMap(ConfigMap.asJava).withFallback(ConfigFactory.defaultOverrides())
  }

  def getContextConfig(adhoc: Boolean, baseConfig: Config = config): Config =
    ConfigFactory.parseMap(Map("context.name" -> "ctx",
                               "context.actorname" -> "ctx",
                               "is-adhoc" -> adhoc.toString).asJava).withFallback(baseConfig)

  lazy val contextConfig = {
    val ConfigMap = Map(
      "context-factory" -> contextFactory,
      "streaming.batch_interval" -> Integer.valueOf(40),
      "streaming.stopGracefully" -> Boolean.box(false),
      "streaming.stopSparkContext" -> Boolean.box(true)
    )
    ConfigFactory
      .parseMap(ConfigMap.asJava)
      .withFallback(config)
      .withFallback(ConfigFactory.defaultOverrides())
  }

  lazy val contextConfigWithGracefulShutdown = {
    val configMap = Map(
      "streaming.stopGracefully" -> Boolean.box(true))
    ConfigFactory.parseMap(configMap.asJava).withFallback(contextConfig)
  }

  def getNewSystem: ActorSystem = ActorSystem("test", config)
}

abstract class JobSpecBaseBase(system: ActorSystem) extends TestKit(system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var inMemoryMetaDAO: MetaDataDAO = _
  var inMemoryBinDAO: BinaryDAO = _
  var daoActor: ActorRef = _
  val emptyActor = system.actorOf(Props.empty)
  var manager: ActorRef = _
  def testJar: java.io.File
  def testEgg: java.io.File
  var supervisor: ActorRef = _
  val timeout: Duration = 5.seconds
  def extrasJar: java.io.File
  lazy val daoConfig: Config = ConfigFactory.load("local.test.combineddao.conf")

  override def afterAll() {
    AkkaTestUtils.shutdownAndWait(manager)
    TestKit.shutdownActorSystem(system)
  }

  protected def uploadBinary(jarFilePath: String,
                             appName: String, binaryType: BinaryType): BinaryInfo = {
    val bytes = scala.io.Source.fromFile(jarFilePath, "ISO-8859-1").map(_.toByte).toArray
    daoActor ! SaveBinary(appName, binaryType, DateTime.now, bytes)
    expectMsg(SaveBinaryResult(Success({})))

    daoActor ! GetLastBinaryInfo(appName)
    expectMsgType[LastBinaryInfo].lastBinaryInfo.get
  }

  protected def uploadTestJar(appName: String = "demo"): BinaryInfo = {
    uploadBinary(testJar.getAbsolutePath, appName, BinaryType.Jar)
  }

  protected def uploadTestEgg(appName: String = "demo"): BinaryInfo = {
    uploadBinary(testEgg.getAbsolutePath, appName, BinaryType.Egg)
  }

  protected def getExtrasJarPath: String = extrasJar.getAbsolutePath

  import CommonMessages._

  val errorEvents: Set[Class[_]] = Set(classOf[JobErroredOut], classOf[JobValidationFailed],
    classOf[NoJobSlotsAvailable], classOf[JobKilled])
  val asyncEvents = Set(classOf[JobStarted])
  val syncEvents = Set(classOf[JobResult])
  val allEvents = errorEvents ++ asyncEvents ++ syncEvents ++ Set(classOf[JobFinished])
}

abstract class JobSpecBase(system: ActorSystem) extends JobSpecBaseBase(system) with TestJarFinder
