package spark.jobserver.japi

import com.typesafe.config.Config
import org.scalactic._
import spark.jobserver.api._

import scala.reflect.ClassTag

case class WrappedJavaValidationException(reason: String, t: Throwable) extends Exception
  with ValidationProblem

/**
  * Internal wrapper for JavaJobs.
  *
  * @param job
  * @param classTag$D$0
  * @param classTag$R$0
  * @param classTag$CX$0
  * @tparam D
  * @tparam R
  * @tparam CX
  */
case class WrappedJavaJob[D: ClassTag, R: ClassTag, CX: ClassTag](job: BaseJavaJob[D, R, CX])
  extends SparkJobBase {

  import spark.jobserver.japi.{WrappedJavaValidationException => JVE}

  override type C = CX
  override type JobData = D
  override type JobOutput = R

  def runJob(sc: CX, runtime: JobEnvironment, data: JobData): JobOutput = {
    job.run(sc, runtime, data)
  }

  def validate(sc: CX, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem] = {
    job.verify(sc, runtime, config) match {
      case j: JVE => Bad(One(j))
      case e: SingleJavaValidationException => Bad(One(JVE(e.issue, e)))
      case e: MultiJavaValidationException =>
        val issues: Seq[JVE] = e.issues.map(issue => JVE(issue, e))
        issues.size match {
          case 0 => Bad(One(JVE("No issues found but MultiJavaValidationException returned",
            new RuntimeException)))
          case 1 => Bad(One(issues.head))
          case _ => Bad(Many(issues.head, issues(1), issues.drop(2): _*))
        }
      case t: Throwable => Bad(One(JVE(t.getMessage, t)))
      case data: JobData => Good(data)
    }
  }
}
