package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.scalactic._
import spark.jobserver.api.{SparkJob => NewSparkJob, _}

/**
  * Remote file cache test job (cluster mode support).
  */
object RemoteDataFileJob extends NewSparkJob {
  type JobData = String
  type JobOutput = String

  def runJob(sc: SparkContext, runtime: JobEnvironment, testFile: JobData): JobOutput = {
    runtime.asInstanceOf[DataFileCache].getDataFile(testFile).getAbsolutePath
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Good(config.getString("testFile"))
  }
}


