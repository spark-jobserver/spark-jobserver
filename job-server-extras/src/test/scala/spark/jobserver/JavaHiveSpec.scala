package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHiveContext
import spark.jobserver.CommonMessages.JobResult
import spark.jobserver.context.{HiveContextLike, JavaContextFactory}
import spark.jobserver.io.JobDAOActor
import spark.jobserver.japi.{BaseJavaJob, JTestHiveJob}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.scalatest._

class JavaTestHiveContextFactory extends JavaContextFactory {
  type C = TestHiveContext with ContextLike

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JTestHiveJob[_]]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = contextFactory(sparkConf)

  protected def contextFactory(conf: SparkConf): C = {
    val sc = new JavaSparkContext(conf)
    Try(new TestHiveContext(sc, false) with HiveContextLike) match {
      case Success(hc) => hc
      case Failure(e) =>
        sc.stop
        throw e
    }
  }
}

object JavaHiveSpec extends JobSpecConfig {
  override val contextFactory = classOf[JavaTestHiveContextFactory].getName
}

@Ignore
class JavaHiveSpec extends ExtrasJobSpecBase(HiveJobSpec.getNewSystem) {

  val classPrefix = "spark.jobserver."
  private val hiveLoaderClass = classPrefix + "JHiveTestLoaderJob"
  private val hiveQueryClass = classPrefix + "JHiveTestJob"

  val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val queryConfig = ConfigFactory.parseString(
    """sql = "SELECT firstName, lastName FROM `default`.`test_addresses` WHERE city = 'San Jose'" """
  )

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
      JavaHiveSpec.getContextConfig(false, JavaHiveSpec.contextConfig)))
  }

  describe("Java Hive Jobs") {
    it("should be able to create a Hive table, then query it using separate Hive-SQL jobs") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(30 seconds, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", hiveLoaderClass, emptyConfig, syncEvents ++ errorEvents)
      expectMsgPF(120 seconds, "Did not get JobResult") {
        case JobResult(_, result: Long) => result should equal (3L)
      }
      expectNoMsg()

      manager ! JobManagerActor.StartJob("demo", hiveQueryClass, queryConfig, syncEvents ++ errorEvents)
      expectMsgPF(6 seconds, "Did not get JobResult") {
        case JobResult(_, result: Array[Row]) =>
          result should have length 2
          result(0)(0) should equal ("Bob")
      }
      expectNoMsg()
    }
  }
}
