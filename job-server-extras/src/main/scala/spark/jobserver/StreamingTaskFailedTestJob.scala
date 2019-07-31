package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

object StreamingTaskFailedTestJob extends SparkStreamingJob {
  def validate(ssc: StreamingContext, config: Config): SparkJobValidation = SparkJobValid

  def runJob(ssc: StreamingContext, config: Config): Any = {
    val queue = mutable.Queue[RDD[String]]()
    val lines = ssc.queueStream(queue)
    val words = lines.flatMap{ l =>
      1/0
      l.split(" ")
    }

    queue += ssc.sparkContext.makeRDD(Seq("123", "test", "test2"))

    val wordCounts = words.countByValue()
    wordCounts.foreachRDD(rdd => rdd.count())
    ssc.start()
    ssc.awaitTermination()
  }
}
