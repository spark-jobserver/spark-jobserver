package spark.jobserver

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import spark.jobserver.common.akka.AkkaTestUtils

/**
 * please note that this test only uses RDDs as named objects, there exists another test class
 * in job-server-extras that uses a combination of RDDs and DataFrames
 */
class NamedObjectsRDDsOnlySpec extends TestKit(ActorSystem("NamedObjectsSpec")) with FunSpecLike
    with ImplicitSender with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  
  val sc = new SparkContext("local[2]", getClass.getSimpleName, new SparkConf)
  val namedObjects: NamedObjects = new JobServerNamedObjects(system)

  implicit def rddPersister[T]: RDDPersister[T] = new RDDPersister[T]

  before {
    namedObjects.getNames().foreach { namedObjects.forget }
  }

  override def afterAll() {
    //ooyala.common.akka.AkkaTestUtils.shutdownAndWait(namedObjManager)
    sc.stop()
    AkkaTestUtils.shutdownAndWait(system)
  }

  describe("NamedObjects") {
    it("get() should return None when RDD does not exist") {
      namedObjects.get("No such RDD") should equal(None)
    }

    it("get() should return Some(RDD) when it exists") {
      val rdd = sc.parallelize(Seq(1, 2, 3))
      namedObjects.update("rdd1", NamedRDD(rdd, true, StorageLevel.MEMORY_ONLY))
      val rdd1 : Option[NamedRDD[Int]] = namedObjects.get("rdd1")
      rdd1 should equal(Some(NamedRDD(rdd, true, StorageLevel.MEMORY_ONLY)))
    }

    it("forget() should do nothing when RDD with given name doesn't exist") {
      namedObjects.update("rdd1", NamedRDD(sc.parallelize(Seq(1, 2, 3)), false, StorageLevel.MEMORY_ONLY))
      namedObjects.get("rdd1") should not equal None
      namedObjects.forget("rdd2")
      namedObjects.get("rdd1") should not equal None
    }

    it("destroy() should destroy an RDD that exists") {
      val rdd1 = NamedRDD(sc.parallelize(Seq(1, 2, 3)), false, StorageLevel.MEMORY_ONLY)
      namedObjects.update("rdd1", rdd1)
      
      namedObjects.get("rdd1") should not equal None
      namedObjects.destroy(rdd1, "rdd1")
      namedObjects.get("rdd1") should equal(None)
    }

    it("getNames() should return names of all managed Objects") {
      namedObjects.getNames().size should equal(0)
      val rdd1 = NamedRDD(sc.parallelize(Seq(1, 2, 3)), false, StorageLevel.MEMORY_ONLY)
      namedObjects.update("rdd1", rdd1)
      namedObjects.update("rdd2", NamedRDD(sc.parallelize(Seq(4, 5, 6)), false, StorageLevel.MEMORY_ONLY))
      namedObjects.getNames().toSeq.sorted should equal(Seq("rdd1", "rdd2"))
      namedObjects.destroy(rdd1, "rdd1")
      namedObjects.getNames().toSeq.sorted should equal(Seq("rdd2"))
    }

    it("getOrElseCreate() should call generator function if RDD does not exist") {
      var generatorCalled = false
      val rdd = namedObjects.getOrElseCreate("rdd1", {
        generatorCalled = true
        NamedRDD(sc.parallelize(Seq(1, 2, 3)), false, StorageLevel.MEMORY_ONLY)
      })
      generatorCalled should equal(true)
    }

    it("getOrElseCreate() should not call generator function, should return existing RDD if one exists") {
      var generatorCalled = false
      val rdd = NamedRDD(sc.parallelize(Seq(1, 2, 3)), true, StorageLevel.MEMORY_ONLY)
      namedObjects.update("rdd2", rdd)
      val rdd2 : NamedRDD[Int] = namedObjects.getOrElseCreate("rdd2", {
        generatorCalled = true
        NamedRDD(sc.parallelize(Seq(4, 5, 6)), true, StorageLevel.MEMORY_ONLY)
      })
      generatorCalled should equal(false)
      rdd2 should equal(rdd)
    }

    it("update() should replace existing RDD") {
      val rdd1 = sc.parallelize(Seq(1, 2, 3))
      val rdd2 = sc.parallelize(Seq(4, 5, 6))
      namedObjects.getOrElseCreate("rdd", NamedRDD(rdd1, true, StorageLevel.MEMORY_ONLY)) should equal(NamedRDD(rdd1, true, StorageLevel.MEMORY_ONLY))
      namedObjects.update("rdd", NamedRDD(rdd2, true, StorageLevel.MEMORY_ONLY))
      namedObjects.get("rdd") should equal(Some(NamedRDD(rdd2, true, StorageLevel.MEMORY_ONLY)))
    }

    it("update() should update an RDD even if it was persisted before") {
      // The result of some computations (ie: a MatrixFactorizationModel after training ALS) might have been
      // persisted before so they might have a specific storageLevel.
      val rdd1 = NamedRDD(sc.parallelize(Seq(1, 2, 3)), false, StorageLevel.MEMORY_ONLY)
      //TODO ???      rdd1.persist(StorageLevel.MEMORY_AND_DISK)
      namedObjects.update("rdd", rdd1)
      namedObjects.get("rdd") should equal(Some(rdd1))
    }

    it("should include underlying exception when error occurs") {
      def errorFunc: NamedRDD[Int] = {
        throw new IllegalArgumentException("boo!")
        NamedRDD(sc.parallelize(Seq(1, 2)), true, StorageLevel.MEMORY_ONLY)
      }
      val err = intercept[RuntimeException] { namedObjects.getOrElseCreate("rdd", errorFunc) }
      err.getClass should equal(classOf[IllegalArgumentException])
    }
    // TODO: Add tests for parallel getOrElseCreate() calls
  }
}
