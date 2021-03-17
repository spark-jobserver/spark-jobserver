package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.scalactic.{Every, Good, Or}
import spark.jobserver.api.{JobEnvironment, ValidationProblem, SparkJob => JobBase}

/**
  * A very short job for stress tests purpose.
  * Small data. Double every value in the data.
  */
object VeryShortDoubleJob extends JobBase {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("VeryShortDoubleJob")
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

  override type JobData = Array[Int]
  override type JobOutput = Array[Int]

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val dd = sc.parallelize(data)
    dd.map(_ * 2).collect()
  }

  override def validate(sc: SparkContext, runtime: JobEnvironment,
                        config: Config): Or[JobData, Every[ValidationProblem]] = Good(Array(1, 2, 3))
}
