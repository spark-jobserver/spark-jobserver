package spark.jobserver.python

import java.security.SecureRandom

import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.scalactic.{Every, Good, Or}
import org.slf4j.LoggerFactory
import spark.jobserver.api.{JobEnvironment, SparkJobBase, ValidationProblem}
import org.spark_project.guava.hash.HashCodes

import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Failure, Success, Try}

case class PythonJob[X <: PythonContextLike](eggPath: String,
                                             modulePath: String,
                                             py4JImports: Seq[String]) extends SparkJobBase {
  override type JobData = Config
  override type JobOutput = Any
  override type C = X

  val logger = LoggerFactory.getLogger(getClass)

  private def endpoint(context: C, contextConfig: Config, jobId: String, jobConfig: Config) = {
    val sparkConf = context.sparkContext.getConf
    JobEndpoint(context, sparkConf, contextConfig, jobId, jobConfig, modulePath, py4JImports)
  }

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
    * Copied from Spark private method in 2.4.0
    */
  def createSecret(conf: SparkConf): String = {
    val bits = conf.getInt("spark.authenticate.secretBitLength", 256)
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](bits / java.lang.Byte.SIZE)
    rnd.nextBytes(secretBytes)
    HashCodes.fromBytes(secretBytes).toString()
  }

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
    val secret = createSecret(sc.sparkContext.getConf)
    val server = new py4j.GatewayServer.GatewayServerBuilder()
      .entryPoint(ep)
      .javaPort(0)
      .authToken(secret)
      .build()
    val pythonPathDelimiter : String = if (System.getProperty("os.name").indexOf("Win") >= 0) ";" else ":"
    val pythonPath = (eggPath +: sc.pythonPath).mkString(pythonPathDelimiter)
    logger.info(s"Using Python path of ${pythonPath}")
    val subProcessOutcome = Try {
      //Server runs asynchronously on a dedicated thread. See Py4J source for more detail
      server.start()
      val process =
        Process(
          Seq(sc.pythonExecutable, "-m", "sparkjobserver.subprocess", server.getListeningPort.toString),
          None,
          "EGGPATH" -> eggPath,
          "PYTHONPATH" -> pythonPath,
          "PYSPARK_PYTHON" -> sc.pythonExecutable,
          "PYSPARK_GATEWAY_SECRET" -> secret)
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
