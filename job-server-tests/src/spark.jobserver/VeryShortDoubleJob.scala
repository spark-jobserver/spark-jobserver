package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._

/**
 * A very short job for stress tests purpose.
 * Small data. Double every value in the data.
 */
object VeryShortDoubleJob extends SparkJob {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "VeryShortDoubleJob")
    val config = ConfigFactory.parseString("")
    validate(sc, config)
      .map(i => runJob(sc, i))
      .foreach(results => println(s"Result is $results"))
  }

  type Tmp = Array[Double]
  override def validate(sc: SparkContext, config: Config): scalaz.Validation[String, Array[Double]] = {
    scalaz.Success(Array(1, 2, 3))
  }

  override def runJob(sc: SparkContext, data: Array[Double]): Any = {
    val dd = sc.parallelize(data)
    dd.map( _ * 2 ).collect()
  }
}
