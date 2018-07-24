package spark.jobserver

import com.google.common.annotations.VisibleForTesting
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.util.Try

@VisibleForTesting
object StreamingTestJob extends SparkStreamingJob {
  def validate(ssc: StreamingContext, config: Config): SparkJobValidation = SparkJobValid

  def runJob(ssc: StreamingContext, config: Config): Any = {
    val maxIterations = Try(config.getInt("streaming.test.job.maxIterations")).getOrElse(1)
    val totalDelaySeconds = Try(config.getInt("streaming.test.job.totalDelaySeconds")).getOrElse(0)
    val shouldPrintCount = Try(config.getBoolean("streaming.test.job.printCount")).getOrElse(true)

    val queue = mutable.Queue[RDD[String]]()
    val lines = ssc.queueStream(queue)
    val words = lines.flatMap{ l =>
      Thread.sleep(totalDelaySeconds * 1000)
      l.split(" ")
    }

    val wordCounts = words.countByValue()
    //do something
    wordCounts.foreachRDD(rdd =>
    try {
        shouldPrintCount match {
          case true => println(rdd.count())
          case false => rdd.count()
        }
      } catch {
        case _: InterruptedException => {}
      })
    ssc.start()
    var totalIterations = 0
    while(totalIterations < maxIterations) {
      queue.synchronized {
        queue += ssc.sparkContext.makeRDD(Seq("123", "test", "test2"))
      }
      totalIterations += 1
      Thread.sleep(100)
    }
    ssc.awaitTermination()
  }
}
