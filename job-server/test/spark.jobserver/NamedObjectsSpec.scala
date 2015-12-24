package spark.jobserver

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestKit }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{ FunSpecLike, FunSpec, BeforeAndAfterAll, BeforeAndAfter }

class NamedObjectsSpec extends TestKit(ActorSystem("NamedObjectsSpec")) with FunSpecLike
    with ImplicitSender with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {
  System.setProperty("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)
  System.setProperty("spark.executor.memory", "512m")
  System.setProperty("spark.akka.threads", Runtime.getRuntime.availableProcessors.toString)

  // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
  System.clearProperty("spark.driver.port")
  System.clearProperty("spark.hostPort")

  val sc = new SparkContext("local[4]", getClass.getSimpleName)
  val namedObjects: NamedObjects = new JobServerNamedObjects(system)

  implicit def rddPersister[T] = new RDDPersister[T]

  class RDDPersister[T] extends NamedObjectPersister[NamedRDD[T]] {
    override def saveToContext(namedObj: NamedRDD[T], name: String) {
      val forceComputation = false
      val storageLevel = StorageLevel.NONE
      require(!forceComputation || storageLevel != StorageLevel.NONE,
        "forceComputation implies storageLevel != NONE")
      namedObj match {
        case NamedRDD(rdd, _) =>
          rdd.setName(name)
          rdd.getStorageLevel match {
            case StorageLevel.NONE => rdd.persist(storageLevel)
            case currentLevel      => rdd.persist(currentLevel)
          }
          // TODO: figure out if there is a better way to force the RDD to be computed
          if (forceComputation) rdd.count()
      }
    }

//    override def getFromContext(c: ContextLike, name: String): NamedRDD[T] = {
//      c.sparkContext.getPersistentRDDs.filter(_._2.name.equals(name)).map(_._2).headOption match {
//        case Some(rdd: RDD[T]) => NamedRDD(rdd, rdd.getStorageLevel)
//      }
//    }

    /**
     * Calls rdd.persist(), which updates the RDD's cached timestamp, meaning it won't get
     * garbage collected by Spark for some time.
     * @param rdd the RDD
     */
    override def refresh(namedRDD: NamedRDD[T]): NamedRDD[T] = namedRDD match {
      case NamedRDD(rdd, _) =>
        rdd.persist(rdd.getStorageLevel)
        namedRDD
    }
  }

  before {
    namedObjects.getNames.foreach { namedObjects.destroy(_) }
  }

  override def afterAll() {
    //ooyala.common.akka.AkkaTestUtils.shutdownAndWait(namedObjManager)
    sc.stop()
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(system)
  }

  describe("NamedObjects") {
    it("get() should return None when RDD does not exist") {
      namedObjects.get("No such RDD") should equal(None)
    }

    it("get() should return Some(RDD) when it exists") {
      val rdd = sc.parallelize(Seq(1, 2, 3))
      namedObjects.update("rdd1", NamedRDD(rdd, StorageLevel.MEMORY_ONLY))
      namedObjects.get("rdd1") should equal(Some(NamedRDD(rdd, StorageLevel.MEMORY_ONLY)))
    }

    it("destroy() should do nothing when RDD with given name doesn't exist") {
      namedObjects.update("rdd1", NamedRDD(sc.parallelize(Seq(1, 2, 3)), StorageLevel.MEMORY_ONLY))
      namedObjects.get("rdd1") should not equal None
      namedObjects.destroy("rdd2")
      namedObjects.get("rdd1") should not equal None
    }

    it("destroy() should destroy an RDD that exists") {
      namedObjects.update("rdd1", NamedRDD(sc.parallelize(Seq(1, 2, 3)), StorageLevel.MEMORY_ONLY))
      namedObjects.get("rdd1") should not equal None
      namedObjects.destroy("rdd1")
      namedObjects.get("rdd1") should equal(None)
    }

    it("getNames() should return names of all managed Objects") {
      namedObjects.getNames().size should equal(0)
      namedObjects.update("rdd1", NamedRDD(sc.parallelize(Seq(1, 2, 3)), StorageLevel.MEMORY_ONLY))
      namedObjects.update("rdd2", NamedRDD(sc.parallelize(Seq(4, 5, 6)), StorageLevel.MEMORY_ONLY))
      namedObjects.getNames().toSeq.sorted should equal(Seq("rdd1", "rdd2"))
      namedObjects.destroy("rdd1")
      namedObjects.getNames().toSeq.sorted should equal(Seq("rdd2"))
    }

    it("getOrElseCreate() should call generator function if RDD does not exist") {
      var generatorCalled = false
      val rdd = namedObjects.getOrElseCreate("rdd1", {
        generatorCalled = true
        NamedRDD(sc.parallelize(Seq(1, 2, 3)), StorageLevel.MEMORY_ONLY)
      })
      generatorCalled should equal(true)
    }

    it("getOrElseCreate() should not call generator function, should return existing RDD if one exists") {
      var generatorCalled = false
      val rdd = NamedRDD(sc.parallelize(Seq(1, 2, 3)), StorageLevel.MEMORY_ONLY)
      namedObjects.update("rdd2", rdd)
      val rdd2 : NamedRDD[Int] = namedObjects.getOrElseCreate("rdd2", {
        generatorCalled = true
        NamedRDD(sc.parallelize(Seq(4, 5, 6)), StorageLevel.MEMORY_ONLY)
      })
      generatorCalled should equal(false)
      rdd2 should equal(rdd)
    }

    it("update() should replace existing RDD") {
      val rdd1 = sc.parallelize(Seq(1, 2, 3))
      val rdd2 = sc.parallelize(Seq(4, 5, 6))
      namedObjects.getOrElseCreate("rdd", NamedRDD(rdd1, StorageLevel.MEMORY_ONLY)) should equal(NamedRDD(rdd1, StorageLevel.MEMORY_ONLY))
      namedObjects.update("rdd", NamedRDD(rdd2, StorageLevel.MEMORY_ONLY))
      namedObjects.get("rdd") should equal(Some(NamedRDD(rdd2, StorageLevel.MEMORY_ONLY)))
    }

    it("update() should update an RDD even if it was persisted before") {
      // The result of some computations (ie: a MatrixFactorizationModel after training ALS) might have been
      // persisted before so they might have a specific storageLevel.
      val rdd1 = NamedRDD(sc.parallelize(Seq(1, 2, 3)), StorageLevel.MEMORY_ONLY)
      //TODO ???      rdd1.persist(StorageLevel.MEMORY_AND_DISK)
      namedObjects.update("rdd", rdd1)
      namedObjects.get("rdd") should equal(Some(rdd1))
    }

    it("should include underlying exception when error occurs") {
      def errorFunc = {
        throw new IllegalArgumentException("boo!")
        NamedRDD(sc.parallelize(Seq(1, 2)), StorageLevel.MEMORY_ONLY)
      }
      val err = intercept[RuntimeException] { namedObjects.getOrElseCreate("rdd", errorFunc) }
      err.getClass should equal(classOf[IllegalArgumentException])
    }
    // TODO: Add tests for parallel getOrElseCreate() calls
  }
}
