package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.sql.cassandra.CassandraSQLContext

/**
 * An empty no computation job for stress tests purpose.
 */
object NoOpJob extends SparkJob {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "NoOpJob")
    val config = ConfigFactory.parseString("")
    val casSql = new CassandraSQLContext(sc)
    val results = runJob(sc, config, casSql)
    println("Result is " + results)
  }

  def validate(sc: SparkContext, config: Config,casSql: CassandraSQLContext): SparkJobValidation = SparkJobValid

  def runJob(sc: SparkContext, config: Config,casSql: CassandraSQLContext): Any = 1
}
