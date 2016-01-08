package spark.jobserver

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestKit }
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, Row }
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{ FunSpecLike, FunSpec, BeforeAndAfterAll, BeforeAndAfter }

/**
 * this Spec is a more complex version of the same one in the job-server project,
 * it uses a combination of RDDs and DataFrames instead of just RDDs
 */
class NamedObjectsSpec extends TestKit(ActorSystem("NamedObjectsSpec")) with FunSpecLike
    with ImplicitSender with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {
  System.setProperty("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)
  System.setProperty("spark.executor.memory", "512m")
  System.setProperty("spark.akka.threads", Runtime.getRuntime.availableProcessors.toString)

  // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
  System.clearProperty("spark.driver.port")
  System.clearProperty("spark.hostPort")

  val sc = new SparkContext("local[4]", getClass.getSimpleName)
  val sqlContext = new SQLContext(sc)
  val namedObjects: NamedObjects = new JobServerNamedObjects(system)

  implicit def rddPersister: NamedObjectPersister[NamedRDD[Int]] = new RDDPersister[Int]
  implicit def dataFramePersister = new DataFramePersister

  val struct = StructType(
    StructField("i", IntegerType, true) ::
      StructField("b", BooleanType, false) :: Nil)

  def rows: RDD[Row] = sc.parallelize(List(Row(1, true), Row(2, false), Row(55, true)))

  before {
    namedObjects.getNames.foreach { namedObjects.destroy(_) }
  }

  override def afterAll() {
    //ooyala.common.akka.AkkaTestUtils.shutdownAndWait(namedObjManager)
    sc.stop()
    ooyala.common.akka.AkkaTestUtils.shutdownAndWait(system)
  }

  describe("NamedObjects") {
    it("get() should return None when object does not exist") {
      namedObjects.get("No such object") should equal(None)
    }

    it("get() should return Some(RDD) and Some(DF) when they exist") {
      val rdd = sc.parallelize(Seq(1, 2, 3))
      namedObjects.update("rdd1", NamedRDD(rdd, true, StorageLevel.MEMORY_ONLY))

      val df = sqlContext.createDataFrame(rows, struct)
      namedObjects.update("df1", NamedDataFrame(df, true, StorageLevel.MEMORY_AND_DISK))

      val rdd1: Option[NamedRDD[Int]] = namedObjects.get("rdd1")
      rdd1 should equal(Some(NamedRDD(rdd, true, StorageLevel.MEMORY_ONLY)))

      val df1: Option[NamedRDD[Int]] = namedObjects.get("df1")
      df1 should equal(Some(NamedDataFrame(df, true, StorageLevel.MEMORY_AND_DISK)))

    }

    it("destroy() should destroy an object that exists") {
      namedObjects.update("rdd1", NamedRDD(sc.parallelize(Seq(1, 2, 3)), false, StorageLevel.MEMORY_ONLY))
      val df = sqlContext.createDataFrame(rows, struct)
      namedObjects.update("df1", NamedDataFrame(df, false, StorageLevel.MEMORY_AND_DISK))

      namedObjects.get("rdd1") should not equal None
      namedObjects.get("df1") should not equal None

      namedObjects.destroy("rdd1")
      namedObjects.get("rdd1") should equal(None)
      //df1 should still be there:
      namedObjects.get("df1") should not equal None
      namedObjects.destroy("df1")
      //now it should be gone as well
      namedObjects.get("df1") should equal(None)

    }

    it("getNames() should return names of all managed Objects") {
      namedObjects.getNames().size should equal(0)
      namedObjects.update("rdd1", NamedRDD(sc.parallelize(Seq(1, 2, 3)), true, StorageLevel.MEMORY_ONLY))
      val df = sqlContext.createDataFrame(rows, struct)
      namedObjects.update("df1", NamedDataFrame(df, true, StorageLevel.MEMORY_AND_DISK))

      namedObjects.getNames().toSeq.sorted should equal(Seq("df1", "rdd1"))
      namedObjects.destroy("rdd1")
      namedObjects.getNames().toSeq.sorted should equal(Seq("df1"))
      namedObjects.destroy("df1")
      namedObjects.getNames().size should equal(0)
    }

    //corresponding test case for RDDs is in job-server project
    it("getOrElseCreate() should call generator function if DataFrame does not exist") {
      var generatorCalled = false
      val df = namedObjects.getOrElseCreate("df1", {
        generatorCalled = true
        NamedDataFrame(sqlContext.createDataFrame(rows, struct), true, StorageLevel.MEMORY_ONLY)
      })
      generatorCalled should equal(true)
      namedObjects.destroy("df1")
    }

    //corresponding test case for RDDs is in job-server project
    it("getOrElseCreate() should not call generator function, should return existing DataFrame") {
      var generatorCalled = false
      val df1 = NamedDataFrame(sqlContext.createDataFrame(rows, struct), true, StorageLevel.MEMORY_ONLY)
      namedObjects.update("df", df1)
      val df2: NamedDataFrame = namedObjects.getOrElseCreate("df", {
        generatorCalled = true
        throw new RuntimeException("ERROR")
      })(None, dataFramePersister)
      generatorCalled should equal(false)
      df2 should equal(df1)
    }

    it("update() should not be bothered by different object types") {
      val rdd = sc.parallelize(Seq(1, 2, 3))
      namedObjects.update("o1", NamedRDD(rdd, true, StorageLevel.MEMORY_ONLY))
      val rdd1: Option[NamedRDD[Int]] = namedObjects.get("o1")
      rdd1 should equal(Some(NamedRDD(rdd, true, StorageLevel.MEMORY_ONLY)))

      val df = sqlContext.createDataFrame(rows, struct)
      namedObjects.update("o1", NamedDataFrame(df, true, StorageLevel.MEMORY_AND_DISK))

      val df1 = {
        val obj: Option[NamedDataFrame] = namedObjects.get("o1")
        obj.get match {
          case NamedDataFrame(df, _, _) => df
        }
      }
      df1 should equal(df)

      namedObjects.destroy("o1")
      namedObjects.get("o1") should equal(None)
    }

    it("update() should replace existing object regardless of type") {
      val df = sqlContext.createDataFrame(rows, struct)
      val rdd2 = sc.parallelize(Seq(4, 5, 6))

      namedObjects.getOrElseCreate("o1", NamedDataFrame(df, true, StorageLevel.MEMORY_AND_DISK)) should equal(NamedDataFrame(df, true, StorageLevel.MEMORY_AND_DISK))
      namedObjects.update("o1", NamedRDD(rdd2, true, StorageLevel.MEMORY_ONLY))
      namedObjects.get("o1") should equal(Some(NamedRDD(rdd2, true, StorageLevel.MEMORY_ONLY)))
    }

    it("should include underlying exception when error occurs") {
      def errorFunc = {
        throw new IllegalArgumentException("boo!")
        NamedDataFrame(sqlContext.createDataFrame(rows, struct), false, StorageLevel.MEMORY_ONLY)
      }
      val err = intercept[RuntimeException] { namedObjects.getOrElseCreate("xx", errorFunc) }
      err.getClass should equal(classOf[IllegalArgumentException])
    }

    it("should create object only once, parallel gets should wait") {
      var obj: Option[NamedObject] = None
      def creatorThread = new Thread {
        override def run {
          //System.err.println("creator started")
          namedObjects.getOrElseCreate("sleep", {
            //wait so that other threads have a chance to start
            Thread.sleep(50)
            val r = NamedDataFrame(sqlContext.createDataFrame(rows, struct), false, StorageLevel.MEMORY_ONLY)
            obj = Some(r)
            //System.err.println("creator finished")
            r
          })(None, dataFramePersister)
        }
      }

      def otherThreads(ix: Int) = new Thread {
        override def run {
          //System.err.println(ix + " started")
          namedObjects.getOrElseCreate("sleep", {
            throw new IllegalArgumentException("boo!")
          })(None, dataFramePersister)
        }
      }
      creatorThread.start
      //give creator thread a bit of a head start
      Thread.sleep(11)
      //now fire more threads
      otherThreads(1).start
      otherThreads(2).start
      otherThreads(3).start
      otherThreads(4).start
      namedObjects.get("sleep") should equal(obj)
    }

  }
}
