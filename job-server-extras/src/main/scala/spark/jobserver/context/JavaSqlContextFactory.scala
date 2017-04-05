package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.ContextLike
import spark.jobserver.japi.{BaseJavaJob, JHiveJob, JSqlJob, JStreamingJob}

class JavaSqlContextFactory extends JavaContextFactory {
  type C = SQLContext with ContextLike

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JSqlJob[_]]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val sc = new SparkContext(sparkConf)
    new SQLContext(sc) with ContextLike {
      def stop(): Unit = this.sparkContext.stop()
    }
  }
}

class JavaHiveContextFactory extends JavaContextFactory {
  type C = HiveContext with ContextLike

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JHiveJob[_]]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    contextFactory(sparkConf)
  }

  protected def contextFactory(conf: SparkConf): C = {
    new HiveContext(new SparkContext(conf)) with HiveContextLike
  }
}

class JavaStreamingContextFactory extends JavaContextFactory {
  type C = StreamingContext with ContextLike

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JStreamingJob[_]]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val interval = config.getInt("streaming.batch_interval")
    val stopGracefully = config.getBoolean("streaming.stopGracefully")
    val stopSparkContext = config.getBoolean("streaming.stopSparkContext")
    new StreamingContext(sparkConf, Milliseconds(interval)) with ContextLike {
      def stop() {
        stop(stopSparkContext, stopGracefully)
      }
    }
  }
}
