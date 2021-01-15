package spark.jobserver.python

import java.io.File
import java.nio.file.Files

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest._
import spark.jobserver.util.JobserverPy4jGateway

import scala.collection.JavaConverters._
import scala.sys.process.Process

object SubprocessSpec {
  def getPythonDir(pathRelativeToSubProject: String): String = {
    /*
      When an sbt test task is run, we might be in the root project directory, or we might
      be specifically inside the python sub-module. Here we determine which and use that
      information to build up absolute paths which can be used for building Python Path
      environment variables.
     */
    val dirIfAtRoot = new File("job-server-python")
    if(dirIfAtRoot.exists) {
      s"${dirIfAtRoot.getAbsolutePath}/$pathRelativeToSubProject"
    } else {
      new File(pathRelativeToSubProject).getAbsolutePath
    }
  }

  lazy val jobServerPath = getPythonDir("src/python")

  lazy val pysparkPath = sys.env.get("SPARK_HOME").map(d => s"$d/python/lib/pyspark.zip")
  lazy val py4jPath = sys.env.get("SPARK_HOME").map(d => s"$d/python/lib/py4j-0.10.7-src.zip")
  lazy val sparkPaths = sys.env.get("SPARK_HOME").map{sh =>
    val pysparkPath = s"$sh/python/lib/pyspark.zip"
    val py4jPath = s"$sh/python/lib/py4j-0.10.7-src.zip"
    Seq(pysparkPath, py4jPath)
  }.getOrElse(Seq())
  lazy val originalPythonPath = sys.env.get("PYTHONPATH")
}

/*
  We're not stitched in to any of the Scala side of the Spark Job Server here,
  so just need a class from which to build object which expose the right
  methods for the python sub process to recognise as an endpoint.
 */
case class TestEndpoint(context: Any,
                        sparkConf: SparkConf,
                        jobConfig: Config,
                        jobClass: String,
                        py4JImports: Seq[String]){

  val jobConfigAsHocon: String = jobConfig.root().render(ConfigRenderOptions.concise())
  val contextConfigAsHocon = jobConfigAsHocon
  val jobId = "ABC"

  var validationProblems: Option[Seq[String]] = None

  def setValidationProblems(problems: java.util.ArrayList[String]): Unit = {
    validationProblems = Some(problems.asScala.toSeq)
  }

  var jobData: Any = _
  def setJobData(data: Any): Unit = {
    jobData = data
  }

  def getJobData: Any = jobData

  var result: Any = _
  def setResult(res: Any): Unit = {
    result = res
  }

  def getPy4JImports: java.util.List[String] = py4JImports.toList.asJava
}

trait IdentifiedContext {
  def contextType: String
}

class SubprocessSpec extends FunSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  import SubprocessSpec._
  val pythonPathDelimiter : String = if (System.getProperty("os.name").indexOf("Win") >= 0) ";" else ":"

  lazy val pythonPath = {

    val pathList = Seq(jobServerPath) ++ sparkPaths ++ originalPythonPath.toSeq
    val p = pathList.mkString(pythonPathDelimiter)
    // Scarman 10-13-2016
    //println(p)
    p
  }

  // creates a stub warehouse dir for derby/hive metastore
  def makeWarehouseDir(): File = {
    val warehouseDir = Files.createTempDirectory("warehouse").toFile()
    warehouseDir.delete()
    warehouseDir
  }

  lazy val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("SubprocessSpec").
    set("spark.sql.shuffle.partitions", "5").
    set("spark.sql.warehouse.dir", makeWarehouseDir().toURI.getPath)
  lazy val sc = new SparkContext(conf)
  lazy val jsc = new JavaSparkContext(sc) with IdentifiedContext{
    def contextType = classOf[JavaSparkContext].getCanonicalName
  }
  lazy val sqlContext = new SQLContext(sc) with IdentifiedContext{
    def contextType = classOf[SQLContext].getCanonicalName
  }
  lazy val hiveContext = new HiveContext(sc) with IdentifiedContext{
    def contextType = classOf[HiveContext].getCanonicalName
  }

  private var jobserverPy4jGateway: JobserverPy4jGateway = _

  before {
     jobserverPy4jGateway = new JobserverPy4jGateway()
  }

  after {
    jobserverPy4jGateway.stop()
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  val sparkContextImports =
    Seq(
      "org.apache.spark.SparkConf",
      "org.apache.spark.api.java.*",
      "org.apache.spark.api.python.*",
      "scala.Tuple2",
      "org.apache.spark.mllib.api.python.*"
    )

  val sqlContextImports = sparkContextImports ++ Seq(
    "org.apache.spark.sql.*"
  )

  val hiveContextImports = sqlContextImports ++ Seq(
    "org.apache.spark.sql.hive.*"
  )

  private def setupPythonProcess(port: String, token: String): scala.sys.process.ProcessBuilder = {
    // Spark-2.4 does not support python >= 3.8 (see https://github.com/apache/spark/pull/26194) leading to
    // failed test cases (TypeError: an integer is required (got type bytes)). If you encounter these issues
    // try to state a python executable < 3.8 explicitly.
    // TODO: remove comment after migration to Spark-3.0
    val pythonExecutable = sys.env.getOrElse("PYTHON_EXECUTABLE", "python3")
    Process(
      Seq(pythonExecutable, "-m", "sparkjobserver.subprocess", port, token),
      None,
      "PYTHONPATH" -> pythonPath,
      "PYSPARK_PYTHON" -> pythonExecutable
    )
  }

  describe("The python subprocess") {

    it("should successfully run a SparkContext based job") {
      val jobConfig = ConfigFactory.parseString("""input.strings = ["a", "a", "b"]""")
      val endpoint =
        TestEndpoint(jsc, conf, jobConfig, "example_jobs.word_count.WordCountSparkJob", sparkContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (0)
      endpoint.result should matchPattern {
        case m: java.util.HashMap[_, _]
          if m.asInstanceOf[java.util.HashMap[String, Int]].asScala.toSeq.sorted == Seq("a" -> 2, "b" -> 1) =>
      }
    }

    it("should capture the validation failures and have a non-zero exit code when input data is invalid") {
      val jobConfig = ConfigFactory.parseString("""""")
      val endpoint =
        TestEndpoint(jsc, conf, jobConfig, "example_jobs.word_count.WordCountSparkJob", sparkContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (1)
      endpoint.validationProblems should be (Some(Seq("config input.strings not found")))
    }

    it("should successfully run a SQLContext based job") {
      val jobConfig = ConfigFactory.parseString(
        """
          |input.data = [
          |  ["bob", 20, 1200],
          |  ["jon", 21, 1400],
          |  ["mary", 20, 1300],
          |  ["sue", 21, 1600]
          |]
        """.stripMargin)
      val endpoint =
        TestEndpoint(sqlContext, conf, jobConfig, "example_jobs.sql_average.SQLAverageJob", sqlContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (0)
      endpoint.result should matchPattern {
        case a: java.util.ArrayList[_]
          if a.asScala.asInstanceOf[Seq[java.util.ArrayList[Any]]].map(_.asScala) ==
            Seq(Seq(20, 1250.0), Seq(21, 1500.0)) =>
      }
    }


    it("should successfully run a HiveContext based job") {
      val jobConfig = ConfigFactory.parseString(
        """
          |input.data = [
          |  ["bob", 20, 1200],
          |  ["jon", 21, 1400],
          |  ["mary", 20, 1300],
          |  ["sue", 21, 1600]
          |]
        """.stripMargin)
      val endpoint =
        TestEndpoint(hiveContext, conf, jobConfig,
          "example_jobs.hive_window.HiveWindowJob", hiveContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (0)
      endpoint.result should matchPattern {
        case a: java.util.ArrayList[_]
          if a.asScala.asInstanceOf[Seq[java.util.ArrayList[Any]]].map(_.asScala) ==
            Seq(
              Seq("bob", 20, 1),
              Seq("mary", 20, 2),
              Seq("jon", 21, 1),
              Seq("sue", 21, 2)
            ) =>
      }
    }

    it("should maintain registered temp tables between two SQL jobs on the same context") {
      val jobConfig = ConfigFactory.parseString(
        """
          |input.data = [
          |  ["bob", 20, 1200],
          |  ["jon", 21, 1400],
          |  ["mary", 20, 1300],
          |  ["sue", 21, 1600]
          |]
        """.stripMargin)
      val endpoint =
        TestEndpoint(sqlContext, conf, jobConfig, "example_jobs.sql_two_jobs.Job1", sqlContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (0)
      endpoint.result should be ("done")
      jobserverPy4jGateway.stop()
      Thread.sleep(5000)

      val jobConfig2 = ConfigFactory.parseString("")
      val endpoint2 =
        TestEndpoint(sqlContext, conf, jobConfig2, "example_jobs.sql_two_jobs.Job2", sqlContextImports)
      val jobserverPy4jGateway2 = new JobserverPy4jGateway()
      val process2 = setupPythonProcess(
        jobserverPy4jGateway2.getGatewayPort(endpoint2), jobserverPy4jGateway2.getToken())
      val pythonExitCode2 = process2.!
      pythonExitCode2 should be (0)
      endpoint2.result should matchPattern {
        case a: java.util.ArrayList[_]
          if a.asScala.asInstanceOf[Seq[java.util.ArrayList[Any]]].map(_.asScala) ==
            Seq(Seq(20, 1250.0), Seq(21, 1500.0)) =>
      }
      jobserverPy4jGateway2.stop()
    }
    
    it("should have non-zero exit code if passed something as a context which is not a context") {

      val jobConfig = ConfigFactory.parseString("""input.strings = ["a", "a", "b"]""")
      val endpoint =
        TestEndpoint(
          NotAContext("context"),
          conf,
          jobConfig,
          "example_jobs.word_count.WordCountSparkJob", sparkContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (2)
    }

    it("should have non-zero exit code if the underlying job fails during run") {
      val jobConfig = ConfigFactory.parseString("""input.strings = ["a", "a", "b"]""")
      val endpoint =
        TestEndpoint(jsc, conf, jobConfig, "example_jobs.failing_job.FailingRunJob", sparkContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (4)
    }

    it("should have non-zero exit code if the underlying job fails during validate") {
      val jobConfig = ConfigFactory.parseString("""input.strings = ["a", "a", "b"]""")
      val endpoint =
        TestEndpoint(jsc, conf, jobConfig, "example_jobs.failing_job.FailingValidateJob", sparkContextImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (3)
    }

    it("should handle cases where a custom context is used") {
      val jobConfig = ConfigFactory.parseString("""input.strings = ["a", "a", "b"]""")
      val customContext = new CustomContext(sc)
      val customImports = sparkContextImports :+ customContext.contextType
      val endpoint =
        TestEndpoint(customContext, conf, jobConfig,
          "example_jobs.custom_context_job.CustomContextJob", customImports)
      val gatewayPort = jobserverPy4jGateway.getGatewayPort(endpoint)
      val process = setupPythonProcess(gatewayPort, jobserverPy4jGateway.getToken())
      val pythonExitCode = process.!
      pythonExitCode should be (0)
      endpoint.result should be ("Hello World 3")
    }
  }
}

case class NotAContext(name: String) {def contextType: String = "notAContext"}

class CustomContext(sparkContext: SparkContext) extends JavaSparkContext(sparkContext) {

  def contextType: String = classOf[CustomContext].getCanonicalName

  def customMethod: String = "Hello World"
}
