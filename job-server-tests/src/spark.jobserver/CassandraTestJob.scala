package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.sql.cassandra.CassandraSQLContext

import scala.util.Try

/**
 * A super-simple Spark job example that implements the SparkJob trait and can be submitted to the job server.
 *
 * Set the config with the sentence to split or count:
 * input.string = "adsfasdf asdkf  safksf a sdfa"
 *
 * validate() returns SparkJobInvalid if there is no input.string
 */
object CassandraTestJob extends SparkJob {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "WordCountExample")
    val config = ConfigFactory.parseString("")
    val casSql = new CassandraSQLContext(sc)
    val results = runJob(sc, config,casSql)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config,casSql: CassandraSQLContext): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config,casSql: CassandraSQLContext): Any = {
    casSql.setKeyspace("spark")
    casSql.cacheTable("spark.people")
    val people = casSql.sql("select * from spark.people")
    people.take(200)


  }
}
