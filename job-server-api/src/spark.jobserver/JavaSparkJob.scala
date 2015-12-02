package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext
import scalaz._

/**
 * A class to make Java jobs easier to write.  In Java:
 * public class MySparkJob extends JavaSparkJob {
 *   @override
 *   public Object runJob(JavaSparkContext jsc, Config jobConfig) { ... }
 * }
 */
abstract class JavaSparkJob extends SparkJobBase {
  type Tmp = Object
  type C = SparkContext

  override def runJob(sc: SparkContext, config: Tmp): Any = {
    runJob(new JavaSparkContext(sc), config)
  }

  override def validate(sc: SparkContext, config: Config): Validation[String, Tmp] = {
    Validation.fromTryCatchNonFatal(parse(new JavaSparkContext(sc), config))
      .leftMap(_.getMessage)
  }

  /**
   * The main class that carries out the Spark job.  The results will be converted to JSON
   * and emitted (but NOT persisted).
   */
  def runJob(jsc: JavaSparkContext, config: Tmp): Any

  /**
   * Parses the config and throws an error in case the config is invalid.
   * The error message will be returned to the user as a 404 HTTP error code.
   */
  def parse(jsc: JavaSparkContext, config: Config): Tmp
}
