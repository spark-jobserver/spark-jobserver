package spark.jobserver.japi

import com.typesafe.config.Config
import org.scalactic._
import spark.jobserver.api._

import scala.reflect.ClassTag

case class JavaValidationException(reason: String, t: Throwable) extends Exception with ValidationProblem

case class JavaJob[R: ClassTag, CX: ClassTag](job: BaseJavaJob[R, CX]) extends SparkJobBase {

  import spark.jobserver.japi.{JavaValidationException => JVE}

  override type C = CX
  override type JobData = Config
  override type JobOutput = R

  def runJob(sc: CX, runtime: JobEnvironment, data: Config): R = {
    job.run(sc, runtime, data)
  }

  def validate(sc: CX, runtime: JobEnvironment, config: Config): Config Or Every[ValidationProblem] = {
    job.verify(sc, runtime, config) match {
      case j : JVE => Bad(One(j))
      case t : Throwable => Bad(One(JVE(t.getMessage, t)))
      case c : Config => Good(c)
    }
  }
}
