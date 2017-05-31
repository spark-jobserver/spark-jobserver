package spark.jobserver.python

import com.typesafe.config.Config
import org.scalactic.{Good, Every, Or}
import org.slf4j.LoggerFactory
import py4j.GatewayServer
import spark.jobserver.api.{SparkJobBase, ValidationProblem, JobEnvironment}

import scala.sys.process.{ProcessLogger, Process}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

case class PythonJob[X <: PythonContextLike](eggPath: String,
                                             modulePath:String,
                                             py4JImports: Seq[String]) extends SparkJobBase {
  override type JobData = Config
  override type JobOutput = Any
  override type C = X

  val logger = LoggerFactory.getLogger(getClass)

  private def endpoint(context: C, contextConfig:Config, jobId: String, jobConfig:Config) = {
    val sparkConf = context.sparkContext.getConf
    JobEndpoint(context, sparkConf, contextConfig, jobId, jobConfig, modulePath, py4JImports)
  }

  def gateway(endpoint: JobEndpoint[C]): GatewayServer = new GatewayServer(endpoint, 0)

  /**
    *
    * To support a useful validate method here for Python jobs we would have call two python processes,
    * one for validate and one for runJob. However this is inefficient and it would mean having to convert
    * JobData into a Java Object and then back out to a Python Object for `runJob`.
    *
    * So for Python Jobs this simply returns indicating the job is valid.
    * Validation by the underlying Python class will be performed within the subprocess called during runJob.
    *
    * @param sc      a SparkContext or similar for the job.  May be reused across jobs.
    * @param runtime the JobEnvironment containing run time information pertaining to the job and context.
    * @param config  the Typesafe Config object passed into the job request
    * @return Always returns the jobConfig, so it will be passed on to runJob as the job data.
    */
  override def validate(sc: X,
                        runtime: JobEnvironment,
                        config: Config): Or[Config, Every[ValidationProblem]] = Good(config)

  /**
    * This is the entry point for a Spark Job Server to execute Python jobs.
    * It calls a Python subprocess to execute the relevant Python Job class.
    *
    * @param sc      a SparkContext or similar for the job.  May be reused across jobs.
    * @param runtime the JobEnvironment containing run time information pertaining to the job and context.
    * @param data    not used for Python jobs
    * @return the job result
    */
  override def runJob(sc: X, runtime: JobEnvironment, data: Config): Any = {
    logger.info(s"Running $modulePath from $eggPath")
    val ep = endpoint(sc, runtime.contextConfig, runtime.jobId, data)
    val server = new GatewayServer(ep, 0)
    val pythonPathDelimiter : String = if(System.getProperty("os.name").indexOf("Win") >= 0) ";" else ":"
    val pythonPath = (eggPath +: sc.pythonPath).mkString(pythonPathDelimiter)
    logger.info(s"Using Python path of ${pythonPath}")
    val subProcessOutcome = Try {
      //Server runs asynchronously on a dedicated thread. See Py4J source for more detail
      server.start()
      val process =
        Process(
          Seq(sc.pythonExecutable, "-m", "sparkjobserver.subprocess", server.getListeningPort.toString),
          None,
          "PYTHONPATH" -> pythonPath,
          "PYSPARK_PYTHON" -> sc.pythonExecutable)
      val err = new StringBuffer
      val procLogger =
        ProcessLogger(
          o => logger.info(s"From Python: $o"),
          e => {logger.error(s"From Python: $e"); err.append(e)})
      val pythonExitCode = process.!(procLogger)
      (pythonExitCode, ep.result) match {

        case (0, Some(rawResult)) =>
          rawResult

        case (0, None) =>
          logger.error(s"Python job ${} ran successfully but failed to write any jobOutput")
          throw new Exception("Python job ran successfully but failed to write any jobOutput")

        case (errorCode, _) =>
          logger.error(s"Python job failed with error code $errorCode")
          throw new Exception(s"Python job failed with error code $errorCode and standard err [$err]")
      }
    }
    server.shutdown()
    subProcessOutcome match {
      case Success(res) => res
      case Failure(ex) => throw ex
    }
  }
}
