package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.sql.cassandra.CassandraSQLContext

/**
 * A very short job for stress tests purpose.
 * Small data. Double every value in the data.
 */
object VeryShortDoubleJob extends SparkJob {
  private val data = Array(1, 2, 3)

  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "VeryShortDoubleJob")
    val config = ConfigFactory.parseString("")
    val casSql = new CassandraSQLContext(sc)
    val results = runJob(sc, config,casSql)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config,casSql: CassandraSQLContext): SparkJobValidation = {
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config,casSql: CassandraSQLContext): Any = {
    val dd = sc.parallelize(data)
    dd.map( _ * 2 ).collect()
  }
}
