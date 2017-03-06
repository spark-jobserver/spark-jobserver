package spark.jobserver.api

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.scalactic._
import spark.jobserver.{ContextLike, NamedObjects}

trait JobEnvironment {
  def jobId: String

  def namedObjects: NamedObjects

  def contextConfig: Config
}

trait ValidationProblem

case class SingleProblem(problem: String) extends ValidationProblem

/**
  * This trait is the main API for Spark jobs submitted to the Job Server.
  *
  * The idea is that validate() will translate the config into another type, and runJob will
  * operate on that type.
  */
trait SparkJobBase {
  type C
  type JobData
  type JobOutput

  /**
    * This is the entry point for a Spark Job Server to execute Spark jobs.
    * This function should create or reuse RDDs and return the result at the end, which the
    * Job Server will cache or display.
    *
    * @param sc      a SparkContext or similar for the job.  May be reused across jobs.
    * @param runtime the JobEnvironment containing run time information pertaining to the job and context.
    * @param data    the JobData returned by the validate method
    * @return the job result
    */
  def runJob(sc: C, runtime: JobEnvironment, data: JobData): JobOutput

  /**
    * This method is called by the job server to allow jobs to validate their input and reject
    * invalid job requests.  If SparkJobInvalid is returned, then the job server returns 400
    * to the user.
    * NOTE: this method should return very quickly.  If it responds slowly then the job server may time out
    * trying to start this job.
    *
    * @param sc      a SparkContext or similar for the job.  May be reused across jobs.
    * @param runtime the JobEnvironment containing run time information pertaining to the job and context.
    * @param config  the Typesafe Config object passed into the job request
    * @return either JobData, which is parsed from config, or a list of validation issues.
    */
  def validate(sc: C, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem]
}

trait SparkJob extends SparkJobBase {
  type C = SparkContext
}

trait OldSparkJob extends SparkJobBase {
  type C = SparkContext
  type JobOutput = Any
  type JobData = Config
}