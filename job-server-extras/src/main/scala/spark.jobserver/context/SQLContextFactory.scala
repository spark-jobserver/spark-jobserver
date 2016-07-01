package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import spark.jobserver.{ContextLike, SparkSqlJob, api}
import spark.jobserver.util.SparkJobUtils

class SQLContextFactory extends ScalaContextFactory {
  type C = SQLContext with ContextLike

  def isValidJob(job: api.SparkJobBase): Boolean = job.isInstanceOf[SparkSqlJob]

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    new SQLContext(new SparkContext(sparkConf)) with ContextLike {
      def stop() { this.sparkContext.stop() }
    }
  }
}