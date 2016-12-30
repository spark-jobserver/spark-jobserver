package spark.jobserver.japi

import com.typesafe.config.Config
import org.scalactic._
import spark.jobserver.api._

import scala.reflect.ClassTag

case class JavaValidationException(reason: String) extends Exception with ValidationProblem

case class JavaJob[R: ClassTag, CX: ClassTag](job: BaseJavaJob[R, CX]) extends SparkJobBase {
  override type C = CX
  override type JobData = Config
  override type JobOutput = R

  def runJob(sc: CX, runtime: JobEnvironment, data: Config): R = {
    job.run(sc, runtime, data)
  }

  def validate(sc: CX, runtime: JobEnvironment, config: Config): Config Or Every[ValidationProblem] = {
    job.verify(sc, runtime, config) match {
      case c: Config => Good(c)
      case e: Exception => Bad(One(JavaValidationException(e.getMessage)))
      case _ => Bad(One(JavaValidationException(":-(")))
    }
  }
}
