package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
<<<<<<< HEAD
import org.apache.spark.sql.hive.test.TestHiveContext
=======
import org.apache.spark.sql.hive.HiveContext
>>>>>>> a41782e... Fix python paths, fix hive spec, make tests run in docker
import spark.jobserver.{ContextLike, SparkHiveJob, api}

class HiveContextFactory extends ScalaContextFactory {
  type C = HiveContext with ContextLike

  def isValidJob(job: api.SparkJobBase): Boolean = job.isInstanceOf[SparkHiveJob]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    contextFactory(sparkConf)
  }

  protected def contextFactory(conf: SparkConf): C = {
    new HiveContext(new SparkContext(conf)) with HiveContextLike
  }
}

private[jobserver] trait HiveContextLike extends ContextLike {
  def stop() { this.sparkContext.stop() }
}
