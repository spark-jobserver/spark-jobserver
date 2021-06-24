package spark.jobserver.japi

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import spark.jobserver.api.JobEnvironment
import spark.jobserver.context.SparkSessionContextLikeWrapper

abstract class JavaSessionJob[D, R] extends BaseJavaJob[D, R, SparkSessionContextLikeWrapper] {

  // Application should use this method to override
  def run(sparkSession: SparkSession, runtime: JobEnvironment, data: D): R

  override def run(sc: SparkSessionContextLikeWrapper, runtime: JobEnvironment, data: D): R = {
    run(sc.spark, runtime, data)
  }

  // Application should use this method to override
  @throws(classOf[RuntimeException])
  def verify(sparkSession: SparkSession, runtime: JobEnvironment, config: Config): D

  override def verify(sc: SparkSessionContextLikeWrapper, runtime: JobEnvironment, config: Config): D = {
    verify(sc.spark, runtime, config)
  }
}

abstract class JSessionJob[R] extends JavaSessionJob[Config, R]

trait JStreamingJob[R] extends BaseJavaJob[Config, R, StreamingContext]

trait JavaStreamingJob[D, R] extends BaseJavaJob[D, R, StreamingContext]