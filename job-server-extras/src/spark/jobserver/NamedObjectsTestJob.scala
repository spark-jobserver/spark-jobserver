package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }

/**
 * A test job for named objects
 */
class NamedObjectsTestJob extends SparkJob with NamedObjectSupport {
  import NamedObjectsTestJobConfig._
  implicit def rddPersister: NamedObjectPersister[NamedRDD[Row]] = new RDDPersister[Row]
  implicit def dataFramePersister: NamedObjectPersister[NamedDataFrame] = new DataFramePersister
  implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  def validate(sql: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  private def rows(sc: SparkContext): RDD[Row] = {
    sc.parallelize(List(Row(1, true), Row(2, false), Row(55, true)))
  }

  def runJob(sc: SparkContext, config: Config): Array[String] = {
    if (config.hasPath(CREATE_DF) && config.getBoolean(CREATE_DF)) {
      val sqlContext = new SQLContext(sc)
      val struct = StructType(
        StructField("i", IntegerType, true) ::
          StructField("b", BooleanType, false) :: Nil)
      val df = sqlContext.createDataFrame(rows(sc), struct)
      namedObjects.update("df1", NamedDataFrame(df, true, StorageLevel.MEMORY_AND_DISK))
    }
    if (config.hasPath(CREATE_RDD) && config.getBoolean(CREATE_RDD)) {
      namedObjects.update("rdd1", NamedRDD(rows(sc), true, StorageLevel.MEMORY_ONLY))
    }

    if (config.hasPath(CREATE_BROADCAST)){
      val broadcast = sc.broadcast(Set(1,2,3,4,5))
      namedObjects.update("broadcast1", NamedBroadcast(broadcast))
    }

    if (config.hasPath(DELETE)) {
      val iter = config.getStringList(DELETE).iterator
      while (iter.hasNext) {
        namedObjects.forget(iter.next)
      }
    }

    //sleep just a bit to allow other threads in
    Thread.sleep(1)
    namedObjects.getNames().toArray
  }
}

object NamedObjectsTestJobConfig {
  val CREATE_DF = "createDF"
  val CREATE_RDD = "createRDD"
  val CREATE_BROADCAST = "createBroadcast"
  val DELETE = "delete"
}

