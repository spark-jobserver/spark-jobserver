package spark.jobserver

import com.typesafe.config.Config
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
object CassandraCacheTest extends SparkJob {


  override def validate(sc: SparkContext, config: Config,casSql: CassandraSQLContext): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config,casSql: CassandraSQLContext): Any = {
    casSql.setKeyspace("spark")
    //casSql.cacheTable("spark.people")
    val people = casSql.sql(config.getString("input.string"))
    people.take(200)


  }
}
