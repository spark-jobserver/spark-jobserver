package spark.jobserver.japi

import com.typesafe.config.Config
import org.apache.spark.api.java.JavaSparkContext
import spark.jobserver.api.JobEnvironment

trait BaseJavaJob[R, C]{
  def run(sc: C, runtime: JobEnvironment, data: Config): R
  def verify(sc: C, runtime: JobEnvironment, config: Config): Config
}

trait JSparkJob[R] extends BaseJavaJob[R, JavaSparkContext]