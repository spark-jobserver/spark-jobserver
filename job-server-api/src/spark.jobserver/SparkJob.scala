package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import scalaz._

/**
 *  This trait is the main API for Spark jobs submitted to the Job Server.
 */
trait SparkJobBase {
  type C
  type Tmp

  /**
   * This is the entry point for a Spark Job Server to execute Spark jobs.
   * This function should create or reuse RDDs and return the result at the end, which the
   * Job Server will cache or display.
   * @param sc a SparkContext or similar for the job.  May be reused across jobs.
   * @param jobConfig the object returned by validate
   * @return the job result
   */
  def runJob(sc: C, jobConfig: Tmp): Any

  /**
   * This method is called by the job server to allow jobs to validate their input and reject
   * invalid job requests. If a Failure is returned, then the job server returns 400
   * to the user.
   * NOTE: this method should return very quickly.  If it responds slowly then the job server may time out
   * trying to start this job.
   * @return Failure or Success, where Success is passed to runJob
   */
  def validate(sc: C, config: Config): Validation[String, Tmp]
}


trait SparkJob extends SparkJobBase {
  type C = SparkContext
}
