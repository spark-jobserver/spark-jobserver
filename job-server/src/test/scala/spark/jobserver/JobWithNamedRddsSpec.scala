package spark.jobserver

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import java.util.concurrent.TimeoutException

import scala.concurrent.duration._

import org.apache.spark.rdd.RDD
import com.typesafe.config.Config
import spark.jobserver.common.akka.AkkaTestUtils

class JobWithNamedRddsSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {

  val sc = new SparkContext("local[4]", getClass.getSimpleName, new SparkConf)

  class TestJob1 extends SparkJob with NamedRddSupport {
    def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

    def runJob(sc: SparkContext, config: Config): Any = {
      1
    }
  }

  val job = new TestJob1
  job.namedObjects = new JobServerNamedObjects(system)
  val namedTestRdds = job.namedRdds

  before {
    namedTestRdds.getNames().foreach { namedTestRdds.destroy }
  }

  override def afterAll() {
    sc.stop()
    AkkaTestUtils.shutdownAndWait(system)
  }

  describe("NamedRdds") {
    it("get() should return None when RDD does not exist") {
      namedTestRdds.getNames().foreach { rddName => namedTestRdds.destroy(rddName) }
      namedTestRdds.get[Int]("No such RDD") should equal(None)
    }

    it("get() should return Some(RDD) when it exists") {
      val rdd = sc.parallelize(Seq(1, 2, 3))
      namedTestRdds.update("rdd1", rdd)
      namedTestRdds.get[Int]("rdd1")(FiniteDuration(8, MILLISECONDS)) should equal(Some(rdd))
    }

    it("get() should ignore timeout when rdd is not known") {
      namedTestRdds.get[Int]("rddXXX")(Duration.Zero) should equal(None)
    }

    it("get() should respect timeout when rdd is known, but not yet available") {

      var rdd : Option[RDD[Int]] = None
      val thread = new Thread {
        override def run() {
          namedTestRdds.getOrElseCreate("rdd-sleep", {
            val t1 = System.currentTimeMillis()
            var x = 0d
            for (i <- 1 to 1000000) { x = x + Math.exp(1d + i) }
            val t2 = System.currentTimeMillis()
            //System.err.println("waking up: " + x + ", duration: " + (t2 - t1))
            val r = sc.parallelize(Seq(1, 2, 3))
            rdd = Some(r)
            r
          })(1.milliseconds)
        }
      }
      thread.start()
      Thread.sleep(11)
      //don't wait
      val err = intercept[TimeoutException] { namedTestRdds.get[Int]("rdd-sleep")(1.milliseconds) }
      err.getClass should equal(classOf[TimeoutException])
      //now wait
      namedTestRdds.get[Int]("rdd-sleep")(FiniteDuration(5, SECONDS)) should equal(Some(rdd.get))
      //clean-up
      namedTestRdds.destroy("rdd-sleep")
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
      def errorFunc: RDD[Int] = {
        throw new IllegalArgumentException("boo!")
        sc.parallelize(Seq(1, 2))
      }
      val err = intercept[RuntimeException] { namedTestRdds.getOrElseCreate("rdd-error", errorFunc) }
      err.getClass should equal(classOf[IllegalArgumentException])
    }
    // TODO: Add tests for parallel getOrElseCreate() calls
  }
}
