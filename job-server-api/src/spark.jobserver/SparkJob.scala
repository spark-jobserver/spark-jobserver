package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext

sealed trait SparkJobValidation {
  // NOTE(harish): We tried using lazy eval here by passing in a function
  // instead, which worked fine with tests but when run with the job-server
  // it would just hang and timeout. This is something worth investigating
  def &&(sparkValidation: SparkJobValidation): SparkJobValidation = this match {
    case SparkJobValid =>  sparkValidation
    case x => x
  }
}
case object SparkJobValid extends SparkJobValidation
case class SparkJobInvalid(reason: String) extends SparkJobValidation

/**
 *  This trait is the main API for Spark jobs submitted to the Job Server.
 */
trait SparkJob {
  /**
   * This is the entry point for a Spark Job Server to execute Spark jobs.
   * This function should create or reuse RDDs and return the result at the end, which the
   * Job Server will cache or display.
   * @param sc a SparkContext for the job.  May be reused across jobs.
   * @param jobConfig the Typesafe Config object passed into the job request
   * @return the job result
   */
  def runJob(sc: SparkContext, jobConfig: Config, casSql: CassandraSQLContext): Any

  /**
   * This method is called by the job server to allow jobs to validate their input and reject
   * invalid job requests.  If SparkJobInvalid is returned, then the job server returns 400
   * to the user.
   * NOTE: this method should return very quickly.  If it responds slowly then the job server may time out
   * trying to start this job.
   * @return either SparkJobValid or SparkJobInvalid
   */
  def validate(sc: SparkContext, config: Config, casSql: CassandraSQLContext): SparkJobValidation
}
