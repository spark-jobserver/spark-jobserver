package spark.jobserver

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.common.akka
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.context.DefaultSparkContextFactory
import spark.jobserver.io.JobDAO

import scala.collection.JavaConverters._
/**
 * Provides a base Config for tests.  Override the vals to configure.  Mix into an object.
 * Also, defaults for values not specified here could be provided as java system properties.
 */
trait JobSpecConfig {

  val JobResultCacheSize = Integer.valueOf(30)
  // number of cores to allocate. Required.
  val NumCpuCores = Integer.valueOf(Runtime.getRuntime.availableProcessors())
  // Executor memory per node, -Xmx style eg 512m, 1G, etc.
  val MemoryPerNode = "512m"
  val MaxJobsPerContext = Integer.valueOf(2)
  def contextFactory: String = classOf[DefaultSparkContextFactory].getName
  lazy val config = {
    val ConfigMap = Map(
      "spark.jobserver.job-result-cache-size" -> JobResultCacheSize,
      "num-cpu-cores" -> NumCpuCores,
      "memory-per-node" -> MemoryPerNode,
      "spark.jobserver.max-jobs-per-context" -> MaxJobsPerContext,
      "spark.jobserver.named-object-creation-timeout" -> "60 s",
      "akka.log-dead-letters" -> Integer.valueOf(0),
      "spark.master" -> "local[*]",
      "context-factory" -> contextFactory,
      "spark.context-settings.test" -> ""
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
    ConfigFactory.parseMap(ConfigMap.asJava).withFallback(ConfigFactory.defaultOverrides())
  }

  def getNewSystem: ActorSystem = ActorSystem("test", config)
}

abstract class JobSpecBaseBase(system: ActorSystem) extends TestKit(system) with ImplicitSender
with FunSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var dao: JobDAO = _
  var daoActor: ActorRef = _
  var manager: ActorRef = _
  def testJar: java.io.File
  var supervisor: ActorRef = _
  def extrasJar: java.io.File

  override def afterAll() {
    AkkaTestUtils.shutdownAndWait(system)
  }

  protected def uploadJar(dao: JobDAO, jarFilePath: String, appName: String) {
    val bytes = scala.io.Source.fromFile(jarFilePath, "ISO-8859-1").map(_.toByte).toArray
    dao.saveJar(appName, DateTime.now, bytes)
  }

  protected def uploadTestJar(appName: String = "demo") { uploadJar(dao, testJar.getAbsolutePath, appName) }

  protected def getExtrasJarPath: String = extrasJar.getAbsolutePath

  import CommonMessages._

  val errorEvents: Set[Class[_]] = Set(classOf[JobErroredOut], classOf[JobValidationFailed],
    classOf[NoJobSlotsAvailable], classOf[JobKilled])
  val asyncEvents = Set(classOf[JobStarted])
  val syncEvents = Set(classOf[JobResult])
  val allEvents = errorEvents ++ asyncEvents ++ syncEvents ++ Set(classOf[JobFinished])
}

abstract class JobSpecBase(system: ActorSystem) extends JobSpecBaseBase(system) with TestJarFinder
