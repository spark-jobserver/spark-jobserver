package spark.jobserver.python

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import spark.jobserver._
import spark.jobserver.api.JobEnvironment
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

case class DummyJobEnvironment(jobId: String, contextConfig: Config) extends JobEnvironment {

  /*
    Do not currently support named objects for Python.

    They involve some tricks with Types which are hard
    to pull off via Py4J.
   */
  override def namedObjects: NamedObjects = new NamedObjects {

    val error = new NotImplementedError("Named objects not supported for PythonJobs")

    override def getOrElseCreate[O <: _root_.spark.jobserver.NamedObject]
    (name: _root_.scala.Predef.String, objGen: => O)
    (implicit timeout: FiniteDuration,
     persister: _root_.spark.jobserver.NamedObjectPersister[O]): O = throw error

    override def update[O <: NamedObject](name: String, objGen: => O)
                                         (implicit timeout: FiniteDuration,
                                          persister: NamedObjectPersister[O]): O = throw error

    override def defaultTimeout: FiniteDuration = throw error

    override def get[O <: NamedObject](name: String)(implicit timeout: FiniteDuration): Option[O] = {
      throw error
    }

    override def forget(name: String): Unit = throw error

    override def destroy[O <: NamedObject](objOfType: O,
                                           name: String)
                                          (implicit persister: NamedObjectPersister[O]): Unit = throw error

    override def getNames(): Iterable[String] = throw error
  }
}

object PythonSparkContextFactorySpec {

  lazy val jobServerPaths = {
    /*
      When an sbt test task is run, we might be in the root project directory, or we might
      be specifically inside the python sub-module. Here we determine which and use that
      information to build up absolute paths which can be used for building Python Path
      environment variables.
     */
    val pathRelativeToSubProject = "target/python/"
    val dirIfAtRoot = new File("job-server-python")
    val targetDir = if(dirIfAtRoot.exists) {
      new File(s"${dirIfAtRoot.getAbsolutePath}/$pathRelativeToSubProject")
    } else {
      new File(s"../${dirIfAtRoot}/" + pathRelativeToSubProject)
    }

    assert(targetDir.exists, s"Target directory should exist. ${targetDir.getAbsolutePath}")
    targetDir.listFiles().filter(_.getName.endsWith(".egg"))
  }

  lazy val jobServerAPIPath = jobServerPaths.filterNot(_.getAbsolutePath.contains("examples")).headOption
  lazy val jobServerAPIExamplePath = jobServerPaths.find(_.getAbsolutePath.contains("examples"))

  lazy val pysparkPath = sys.env.get("SPARK_HOME").map(d => s"$d/python/lib/pyspark.zip")
  lazy val py4jPath  = sys.env.get("SPARK_HOME").map(d => s"$d/python/lib/py4j-0.9-src.zip")
  lazy val originalPythonPath  = sys.env.get("PYTHONPATH")

  case object DummyJobCache extends JobCache {

    override def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo =
      sys.error("Not Implemented")

    override def getJavaJob(appName: String, uploadTime: DateTime, classPath: String): JavaJarInfo =
      sys.error("No Implemented :(")

    override def getPythonJob(appName: String, uploadTime: DateTime, classPath: String): PythonJobInfo = {
      val path =
        if(appName == "test") {
          "/tmp/test.egg"
        } else {
          jobServerAPIExamplePath.getOrElse(sys.error("job server examples path not found")).getAbsolutePath
        }
        PythonJobInfo(path)
    }

  }

  lazy val config = ConfigFactory.parseString(
    s"""
      |python.paths = [
      |  "${jobServerAPIPath.getOrElse(sys.error("job server egg not found"))}",
      |  "${pysparkPath.getOrElse("")}",
      |  "${py4jPath.getOrElse("")}",
      |  "${originalPythonPath.getOrElse("")}"
      |]
      |
      |python.executable = "python"
    """.replace("\\","\\\\") // Windows-compatibility
      .stripMargin)

  lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("PythonSparkContextFactorySpec")
}

class PythonSparkContextFactorySpec extends FunSpec with Matchers with BeforeAndAfter {

  import PythonSparkContextFactorySpec._

  var context: JavaSparkContext with PythonContextLike = null

  after {
    if(context != null) {
      context.stop()
    }
  }

  describe("PythonSparkContextFactory") {

    it("should create PythonSparkContexts") {
      val factory = new PythonSparkContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      context shouldBe a[JavaSparkContext with PythonContextLike]
    }

    it("should create JobContainers") {
      val factory = new PythonSparkContextFactory()
      val result = factory.loadAndValidateJob("test", DateTime.now(), "path.to.Job", DummyJobCache)
      result.isGood should be (true)
      val jobContainer = result.get
      jobContainer shouldBe an[PythonJobContainer[_]]
      jobContainer.getSparkJob should be (
        PythonJob("/tmp/test.egg", "path.to.Job", PythonContextFactory.sparkContextImports))
    }

    def runTest(factory: PythonSparkContextFactory,
                context: JavaSparkContext with PythonContextLike,
                c:Config): Unit = {
      val loadResult = factory.loadAndValidateJob(
        "word-count",
        DateTime.now(),
        "example_jobs.word_count.WordCountSparkJob",
        DummyJobCache)
      loadResult.isGood should be (true)
      val jobContainer = loadResult.get
      val job = jobContainer.getSparkJob
      val jobConfig = ConfigFactory.parseString(
        """
          |input.strings = ["a", "b", "b", "c", "a", "b"]
        """.stripMargin)
      val jobEnv = DummyJobEnvironment("1234", c)
      val jobDataOrProblem = job.validate(context, jobEnv, jobConfig)
      jobDataOrProblem.isGood should be (true)
      val jobData = jobDataOrProblem.get
      val result = job.runJob(context, jobEnv, jobData)
      result should matchPattern {
        case m: java.util.Map[_, _] if m.asScala.toMap == Map("a" -> 2, "b" -> 3, "c" -> 1) =>
      }
    }

    it("should return jobs which can be successfully run") {
      val factory = new PythonSparkContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      runTest(factory, context, config)
    }

    it("should successfully run jobs using python3", WindowsIgnore) {
      val factory = new PythonSparkContextFactory()
      val p3Config = ConfigFactory.parseString(
        """
          |python.executable = "python3"
        """.stripMargin).withFallback(config)
      context = factory.makeContext(sparkConf, p3Config, "test-create")
      runTest(factory, context, p3Config)
    }

    def runFailingTest(factory: PythonSparkContextFactory,
                       context: JavaSparkContext with PythonContextLike,
                       c:Config): Unit = {
      val loadResult = factory.loadAndValidateJob(
        "word-count",
        DateTime.now(),
        "example_jobs.word_count.FailingSparkJob",
        DummyJobCache)
      loadResult.isGood should be (true)
      val jobContainer = loadResult.get
      val job = jobContainer.getSparkJob
      val jobConfig = ConfigFactory.parseString(
        """
          |input.strings = ["a", "b", "b", "c", "a", "b"]
        """.stripMargin)
      val jobEnv = DummyJobEnvironment("1234", c)
      val jobDataOrProblem = job.validate(context, jobEnv, jobConfig)
      jobDataOrProblem.isGood should be (true)
      val jobData = jobDataOrProblem.get
      try {
        job.runJob(context, jobEnv, jobData)
        assert(false)
      } catch {
        case err: Exception => err.getMessage.contains("Deliberate failure") should be (true)
      }
    }

    it("should return job error messages") {
      val factory = new PythonSparkContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      runFailingTest(factory, context, config)
    }
  }
}
