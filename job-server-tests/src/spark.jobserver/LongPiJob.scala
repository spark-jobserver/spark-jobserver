package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import java.util.{Random, Date}
import org.apache.spark._
import org.scalactic._
import scala.util.Try

import spark.jobserver.api._

/**
 * A long job for stress tests purpose.
 * Iterative and randomized algorithm to compute Pi.
 * Imagine a square centered at (1,1) of length 2 units.
 * It tightly encloses a circle centered also at (1,1) of radius 1 unit.
 * Randomly throw darts to them.
 * We can use the ratio of darts inside the square and circle to approximate the Pi.
 *
 * stress.test.longpijob.duration controls how long it run in seconds.
 * Longer duration increases precision.
 *
 */
object LongPiJob extends api.SparkJob {
  private val rand = new Random(now)

  type JobData = Int
  type JobOutput = Double


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("LongPiJob")
    val sc = new SparkContext(conf)
    val env = new JobEnvironment {
      def jobId: String = "abcdef"
      //scalastyle:off
      def namedObjects: NamedObjects = ???
      def contextConfig: Config = ConfigFactory.empty
    }
    val results = runJob(sc, env, 5)
    println("Result is " + results)
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
    JobData Or Every[ValidationProblem] = {
    val duration = Try(config.getInt("stress.test.longpijob.duration")).getOrElse(5)
    Good(duration)
  }

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val duration = data
    var hit = 0L
    var total = 0L
    val start = now
    while(stillHaveTime(start, duration)) {
      val counts = estimatePi(sc)
      hit += counts._1
      total += counts._2
    }

    (4.0 * hit) / total
  }

  /**
   *
   * @param sc
   * @return (hit, total) where hit is the count hit inside circle and total is the total darts
   */
  private def estimatePi(sc: SparkContext): Tuple2[Int, Int] = {
    val data = Array.iterate(0, 1000)(x => x + 1)

    val dd = sc.parallelize(data)
    dd.map { x =>
      // The first value is the count of hitting inside the circle. The second is the total.
      if (throwADart()) (1, 1) else (0, 1)
    }.reduce { (x, y) => (x._1 + y._1, x._2 + y._2) }
  }

  /**
   * Throw a dart.
   *
   * @return true if the dart hits inside the circle.
   */
  private def throwADart(): Boolean = {
    val x = rand.nextDouble() * 2
    val y = rand.nextDouble() * 2
    // square of distance to center
    val dist = math.pow(x - 1, 2) + math.pow(y - 1, 2)
    // square root wouldn't affect the math.
    // if dist > 1, then hit outside the circle, else hit inside the circle
    dist <= 1
  }

  private def now(): Long = (new Date()).getTime

  private val OneSec = 1000 // in milliseconds
  private def stillHaveTime(startTime: Long, duration: Int): Boolean = (now - startTime) < duration * OneSec
}
