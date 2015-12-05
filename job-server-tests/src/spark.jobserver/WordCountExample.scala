package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.util.Try

/**
 * A super-simple Spark job example that implements the SparkJob trait and can be submitted to the job server.
 *
 * Set the config with the sentence to split or count:
 * input.string = "adsfasdf asdkf  safksf a sdfa"
 *
 * validate() returns SparkJobInvalid if there is no input.string
 */
object WordCountExample extends SparkJob {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "WordCountExample")
    val config = ConfigFactory.parseString("")
    validate(sc, config)
      .map(i => runJob(sc, i))
      .foreach(results => println(s"Result is $results"))
  }

  type Tmp = Seq[String]
  override def validate(sc: SparkContext, config: Config): scalaz.Validation[String, Seq[String]] = {
    scalaz.Validation.fromTryCatchNonFatal(config.getString("input.string"))
      .leftMap(_.getMessage)
      .map(_.split(" ").toSeq)
  }

  override def runJob(sc: SparkContext, data: Seq[String]): Any = {
    val dd = sc.parallelize(data)
    dd.map((_, 1)).reduceByKey(_ + _).collect().toMap
  }
}
