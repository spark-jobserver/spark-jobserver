package spark.jobserver.japi

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import spark.jobserver.api.JobEnvironment

class JavaValidationException extends RuntimeException

case class SingleJavaValidationException(issue: String) extends JavaValidationException

case class MultiJavaValidationException(issues: Seq[String]) extends JavaValidationException

/**
  * This trait is the main API for Java jobs submitted to the Job Server. To implement a Java Job, see
  * `JSparkJob` and `JavaSparkJob`.
  *
  * The idea is that validate() will translate the config into another type, and runJob will
  * operate on that type.
  *
  * @tparam D the return type of `validate` and input for `runJob`
  * @tparam R the return type of `runJob`
  * @tparam C the SparkContext base class
  */
trait BaseJavaJob[D, R, C] {

  /**
    * This is the entry point for a Spark Job Server to execute Java jobs.
    * This function should create or reuse RDDs and return the result at the end, which the
    * Job Server will cache or display.
    *
    * @param sc      a SparkContext or similar for the job.  May be reused across jobs.
    * @param runtime the JobEnvironment containing run time information pertaining to the job and context.
    * @param data    the JobData returned by the validate method
    * @return the job result
    */
  def run(sc: C, runtime: JobEnvironment, data: D): R

  /**
    * This method is called by the job server to allow jobs to validate their input and reject
    * invalid job requests. If the configuration is valid, the input should converted to `C`. Otherwise,
    * an exception should be thrown to indicate invalid configurations.
    * NOTE: this method should return very quickly.  If it responds slowly then the job server may time out
    * trying to start this job.
    *
    * @param sc      a SparkContext or similar for the job.  May be reused across jobs.
    * @param runtime the JobEnvironment containing run time information pertaining to the job and context.
    * @param config  the Typesafe Config object passed into the job request
    * @throws Throwable to indicate invalid configurations
    * @return C, which is parsed from config
    */
  @throws(classOf[RuntimeException])
  def verify(sc: C, runtime: JobEnvironment, config: Config): D
}

/**
  * Convenience interface where validate always returns the input `Config`.
  *
  * @tparam R the return type of `runJob`
  */
trait JSparkJob[R] extends BaseJavaJob[Config, R, JavaSparkContext]


/**
  * Convenience interface where the return type of `validate` and `runJob` can be specified.
  *
  * @tparam D the return type of `validate` and input for `runJob`
  * @tparam R the return type of `runJob`
  */
trait JavaSparkJob[D, R] extends BaseJavaJob[D, R, JavaSparkContext]