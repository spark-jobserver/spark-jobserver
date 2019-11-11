package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, ValidationProblem}
import spark.jobserver.context.SparkSessionContextLikeWrapper

trait SparkSessionJob extends spark.jobserver.api.SparkJobBase {
  type C = SparkSessionContextLikeWrapper

  // Application should use this method to override
  def runJob(sparkSession: SparkSession, runtime: JobEnvironment, data: JobData): JobOutput

  override def runJob(sc: C, runtime: JobEnvironment, data: JobData): JobOutput
   = runJob(sc.spark, runtime, data)

  // Application should use this method to override
  def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
      JobData Or Every[ValidationProblem]

  override def validate(sc: C, runtime: JobEnvironment, config: Config):
      JobData Or Every[ValidationProblem]
    = validate(sc.spark, runtime, config)
}

