package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.scalactic.{Every, Good, Or}
import spark.jobserver.api.{JobEnvironment, ValidationProblem, SparkJob => JobBase}

/**
  * An empty no computation job for stress tests purpose.
  */
object NoOpJob extends JobBase {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("NoOpJob")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    val env = new JobEnvironment {
      def jobId: String = "abcdef"

      //scalastyle:off
      def namedObjects: NamedObjects = ???

      def contextConfig: Config = ConfigFactory.empty
    }
    val input = validate(sc, env, config)
    val results = runJob(sc, env, input.get)
    println("Result is " + results)
  }

  override type JobData = Int
  override type JobOutput = Int

  override def validate(sc: SparkContext, runtime: JobEnvironment,
                        config: Config): Or[JobData, Every[ValidationProblem]] = Good(1)

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = 1

}
