package spark.jobserver.python

import com.typesafe.config.{ConfigRenderOptions, Config}
import org.apache.spark.SparkConf
import spark.jobserver.api.JobEnvironment
import scala.collection.JavaConverters._

/**
  * The target for all communications between the Spark Job Server and the underlying python subprocess.
  *
  * When a Py4J gateway is created, it is passed an object of type `Any` which is available to the
  * python process as a field on the gateway named `endpoint`. Since the object is of type `Any`,
  * it is up to the python program to know which members to expect the endpoint to have.
  *
  * The Spark Job Server python subprocess assumes the endpoint to be an implementation of this Trait,
  * and attempts to access fields and methods accordingly.
  */
case class JobEndpoint[C <: PythonContextLike](context: C,
                                               sparkConf: SparkConf,
                                               contextConfig: Config,
                                               jobId: String,
                                               jobConfig: Config,
                                               jobClass: String,
                                               py4JImports: Seq[String]
                                              ){

  /**
    * @return The contextConfig, which is a Typesafe Config object, serialized to HOCON,
    *         so that it can be deserialized by the Python implementation of Typesafe
    *         Config (pyhocon) in the subprocess.
    */
  def contextConfigAsHocon: String = contextConfig.root().render(ConfigRenderOptions.concise())

  /**
    * @return The jobConfig, which is a Typesafe Config object, serialized to HOCON,
    *         so that it can be deserialized by the Python implementation of Typesafe
    *         Config (pyhocon) in the subprocess.
    */
  def jobConfigAsHocon: String = jobConfig.root().render(ConfigRenderOptions.concise())

  var validationProblems: Option[Seq[String]] = None

  /**
    * Receives the list of validation problems from the python subprocess.
    *
    * @param problems A list of problems. This is of type `java.util.ArrayList[String]`
    *                 because this is the format that Python lists are converted to
    *                 by Py4J. This is converted to a Scala Seq internally.
    */
  def setValidationProblems(problems: java.util.ArrayList[String]): Unit = {
    validationProblems = Some(problems.asScala.toSeq)
  }

  var jobData: Option[Any] = None

  /**
    * Method for the python subprocess to return jobdata after validate stage
 *
    * @param data the job data to be held for the run stage
    */
  def setJobData(data: Any): Unit = {
    jobData = Some(data)
  }

  /**
    * Method for python subprocess to retrieve job data in run stage.
 *
    * @return the jobData if it exists, else null
    */
  def getJobData: Any = jobData.getOrElse(null)

  var result: Option[Any] = None

  /**
    * Receives the result from the python subprocess.
    *
    * @param res The result of the process. Although this can be `Any`, if it cannot
    *            be serialized by Spray Json then an error will occur when attempting
    *            to return this result to the client.
    */
  def setResult(res: Any): Unit = {
    result = Some(res)
  }

  /**
    *
    * @return Returns the list of py4J imports as a Java list, which is
    *         the necessary type to have the list auto-converted to a
    *         python list.
    */
  def getPy4JImports: java.util.List[String] = py4JImports.toList.asJava
}
