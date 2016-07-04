package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import spark.jobserver.{api, ContextLike, SparkHiveJob}
import spark.jobserver.util.SparkJobUtils

class HiveContextFactory extends ScalaContextFactory {
  type C = HiveContext with ContextLike

  def isValidJob(job: api.SparkJobBase): Boolean = job.isInstanceOf[SparkHiveJob]

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    contextFactory(sparkConf)
  }

  protected def contextFactory(conf: SparkConf): C = {
    new HiveContext(new SparkContext(conf)) with HiveContextLike
  }
}

private[jobserver] trait HiveContextLike extends ContextLike {
  def stop() { this.sparkContext.stop() }
}
