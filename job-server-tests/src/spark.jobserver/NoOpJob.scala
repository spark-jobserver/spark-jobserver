package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._

/**
 * An empty no computation job for stress tests purpose.
 */
object NoOpJob extends SparkJob {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "NoOpJob")
    val config = ConfigFactory.parseString("")
    validate(sc, config)
      .map(i => runJob(sc, i))
      .foreach(results => println(s"Result is $results"))
  }

  type Tmp = Unit
  def validate(sc: SparkContext, config: Config): scalaz.Validation[String, Unit] =
    scalaz.Success(())

  def runJob(sc: SparkContext, config: Unit): Any = 1
}
