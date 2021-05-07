package spark.jobserver.stress

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await
import spark.jobserver._
import spark.jobserver.io.JobDAOActor.{GetLastBinaryInfo, JobResult, LastBinaryInfo, SaveBinary}
import spark.jobserver.io.{BinaryInfo, BinaryType, InMemoryBinaryDAO, InMemoryMetaDAO, JobDAOActor}
import spark.jobserver.util.JobserverTimeouts

import java.time.ZonedDateTime

/**
 * A stress test for launching many jobs within a job context
 * Launch using sbt> test:run
 * Watch with visualvm to see memory usage
 *
 * TODO(velvia): Turn this into an actual test.  For now it's an app, requires manual testing.
 */
object SingleContextJobStress extends App with TestJarFinder {

  import JobManagerActor._
  import scala.concurrent.duration._
  val jobDaoPrefix = "target/scala-" + version + "/jobserver/"
  val config = ConfigFactory.parseString("""
    num-cpu-cores = 4           # Number of cores to allocate.  Required.
    memory-per-node = 512m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
    """)

  val system = ActorSystem("test", config)
  // Stuff needed for futures and Await
  implicit val ec = system
  implicit val ShortTimeout = Timeout(3 seconds)

  val jobDaoDir = jobDaoPrefix + ZonedDateTime.now.toString()
  lazy val daoConfig: Config = ConfigFactory.load("local.test.dao.conf")
  val inMemoryMetaDAO = new InMemoryMetaDAO
  val inMemoryBinDAO = new InMemoryBinaryDAO
  val daoActor = system.actorOf(JobDAOActor.props(inMemoryMetaDAO, inMemoryBinDAO, daoConfig))

  val jobManager = system.actorOf(Props(classOf[JobManagerActor], daoActor, "c1", "local[4]", config, false))

  private def uploadJar(jarFilePath: String, appName: String): BinaryInfo = {
    val bytes = scala.io.Source.fromFile(jarFilePath, "ISO-8859-1").map(_.toByte).toArray
    Await.result(daoActor ? SaveBinary(appName, BinaryType.Jar, ZonedDateTime.now, bytes),
      JobserverTimeouts.DAO_DEFAULT_TIMEOUT)
    Await.result(daoActor ? GetLastBinaryInfo(appName), JobserverTimeouts.DAO_DEFAULT_TIMEOUT).
      asInstanceOf[LastBinaryInfo].lastBinaryInfo.get
  }

  private val demoJarPath = testJar.getAbsolutePath
  private val demoJarClass = "spark.jobserver.WordCountExample"
  private val emptyConfig = ConfigFactory.parseString("")

  // Create the context
  val res1 = Await.result(jobManager ? Initialize, 3 seconds)
  assert(res1.getClass == classOf[Initialized])

  val testBinInfo = uploadJar(demoJarPath, "demo1")

  // Now keep running this darn test ....
  var numJobs = 0
  val startTime = System.currentTimeMillis()

  while (true) {
    val f = jobManager ? StartJob(demoJarClass, Seq(testBinInfo), emptyConfig, Set(classOf[JobResult]))
    Await.result(f, 3 seconds) match {
      case JobResult(Some(m)) =>
        numJobs += 1
        if (numJobs % 100 == 0) {
          val elapsed = System.currentTimeMillis() - startTime
          println("%d jobs finished in %f seconds".format(numJobs, elapsed / 1000.0))
        }
      case x =>
        println("Some error occurred: " + x)
        sys.exit(1)
    }
    // Thread sleep 1000
  }
}
