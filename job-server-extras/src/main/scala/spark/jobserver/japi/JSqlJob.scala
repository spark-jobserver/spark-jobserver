package spark.jobserver.japi

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import spark.jobserver.api.JobEnvironment
import spark.jobserver.context.SparkSessionContextLikeWrapper

abstract class JSessionJob[R] extends BaseJavaJob[R, SparkSessionContextLikeWrapper] {

  // Application should use this method to override
  def run(sparkSession: SparkSession, runtime: JobEnvironment, data: Config): R

  override def run(sc: SparkSessionContextLikeWrapper, runtime: JobEnvironment, data: Config): R = {
    run(sc.spark, runtime, data)
  }

  // Application should use this method to override
  def verify(sparkSession: SparkSession, runtime: JobEnvironment, config: Config): Config

  override def verify(sc: SparkSessionContextLikeWrapper, runtime: JobEnvironment, config: Config): Config = {
    verify(sc.spark, runtime, config)
  }
}

trait JStreamingJob[R] extends BaseJavaJob[R, StreamingContext]