package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.ContextLike
import spark.jobserver.japi.{BaseJavaJob, JSparkJob, JavaJob}

class JavaSparkContextFactory extends JavaContextFactory {
  type C = JavaSparkContext with ContextLike

  def isValidJob(job: BaseJavaJob[_, _]): Boolean = job.isInstanceOf[JSparkJob[_]]

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    new JavaSparkContext(sparkConf) with ContextLike {
      override def sparkContext: SparkContext = this.sc
    }
  }
}
