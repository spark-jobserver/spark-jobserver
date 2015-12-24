package spark.jobserver

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestKit }
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{ FunSpecLike, FunSpec, BeforeAndAfterAll, BeforeAndAfter }
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages.{ JobErroredOut, JobResult }

class JobWithNamedRddsSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {

  System.setProperty("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)
  System.setProperty("spark.executor.memory", "512m")
  System.setProperty("spark.akka.threads", Runtime.getRuntime.availableProcessors.toString)

  // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
  System.clearProperty("spark.driver.port")
  System.clearProperty("spark.hostPort")

  private val emptyConfig = ConfigFactory.parseString("spark.master = bar")

  val sc = new SparkContext("local[4]", getClass.getSimpleName)

  class TestJob1 extends SparkJob with NamedRddSupport {
    def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

    def runJob(sc: SparkContext, config: Config): Any = {
      1
    }
  }

  val job = new TestJob1

  if (job.isInstanceOf[NamedObjectSupport]) {
    val namedObjects = job.asInstanceOf[NamedObjectSupport].namedObjectsPrivate
    if (namedObjects.get() == null) {
      namedObjects.compareAndSet(null, new JobServerNamedObjects(system))
    }
  }
  val namedTestRdds = job.namedRdds

  override def afterAll() {
    sc.stop()
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(system)
  }

  describe("NamedRdds") {
    it("get() should return None when RDD does not exist") {
      namedTestRdds.getNames.foreach { rddName => namedTestRdds.destroy(rddName) }
      namedTestRdds.get[Int]("No such RDD") should equal(None)
    }

    it("get() should return Some(RDD) when it exists") {
      val rdd = sc.parallelize(Seq(1, 2, 3))
      namedTestRdds.update("rdd1", rdd)
      namedTestRdds.get[Int]("rdd1") should equal(Some(rdd))
    }

    it("destroy() should do nothing when RDD with given name doesn't exist") {
      namedTestRdds.update("rdd1", sc.parallelize(Seq(1, 2, 3)))
      namedTestRdds.get[Int]("rdd1") should not equal None
      namedTestRdds.destroy("rdd2")
      namedTestRdds.get[Int]("rdd1") should not equal None
    }

    it("destroy() should destroy an RDD that exists") {
      namedTestRdds.update("rdd1", sc.parallelize(Seq(1, 2, 3)))
      namedTestRdds.get[Int]("rdd1") should not equal None
      namedTestRdds.destroy("rdd1")
      namedTestRdds.get[Int]("rdd1") should equal(None)
    }

    it("getNames() should return names of all managed RDDs") {
      namedTestRdds.getNames().size should equal(0)
      namedTestRdds.update("rdd1", sc.parallelize(Seq(1, 2, 3)))
      namedTestRdds.update("rdd2", sc.parallelize(Seq(4, 5, 6)))
      namedTestRdds.getNames().toSeq.sorted should equal(Seq("rdd1", "rdd2"))
      namedTestRdds.destroy("rdd1")
      namedTestRdds.getNames().toSeq.sorted should equal(Seq("rdd2"))
    }

    it("getOrElseCreate() should call generator function if RDD does not exist") {
      var generatorCalled = false
      val rdd = namedTestRdds.getOrElseCreate("rdd1", {
        generatorCalled = true
        sc.parallelize(Seq(1, 2, 3))
      })
      generatorCalled should equal(true)
    }

    it("getOrElseCreate() should not call generator function, should return existing RDD if one exists") {
      var generatorCalled = false
      val rdd = sc.parallelize(Seq(1, 2, 3))
      namedTestRdds.update("rdd1", rdd)
      val rdd2 = namedTestRdds.getOrElseCreate("rdd1", {
        new Throwable().printStackTrace()
        generatorCalled = true
        sc.parallelize(Seq(4, 5, 6))
      })
      generatorCalled should equal(false)
      rdd2 should equal(rdd)
    }

    it("update() should replace existing RDD") {
      val rdd1 = sc.parallelize(Seq(1, 2, 3))
      val rdd2 = sc.parallelize(Seq(4, 5, 6))
      namedTestRdds.getOrElseCreate("rdd", rdd1) should equal(rdd1)
      namedTestRdds.update("rdd", rdd2)
      namedTestRdds.get[Int]("rdd") should equal(Some(rdd2))
    }

    it("update() should update an RDD even if it was persisted before") {
      // The result of some computations (ie: a MatrixFactorizationModel after training ALS) might have been
      // persisted before so they might have a specific storageLevel.
      val rdd1 = sc.parallelize(Seq(1, 2, 3))
      rdd1.persist(StorageLevel.MEMORY_AND_DISK)
      namedTestRdds.update("rdd", rdd1)
      namedTestRdds.get[Int]("rdd") should equal(Some(rdd1))
    }

    it("should include underlying exception when error occurs") {
      def errorFunc = {
        throw new IllegalArgumentException("boo!")
        sc.parallelize(Seq(1, 2))
      }
      val err = intercept[RuntimeException] { namedTestRdds.getOrElseCreate("rdd-error", errorFunc) }
      err.getClass should equal(classOf[IllegalArgumentException])
    }
    // TODO: Add tests for parallel getOrElseCreate() calls
  }
}
