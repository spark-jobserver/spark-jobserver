package spark.jobserver.python

import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import java.nio.file.Files
import java.nio.file.Paths

import akka.testkit.{TestKit, TestProbe}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import spark.jobserver.{JavaStreamingSpec, JobsLoader, WindowsIgnore}
import spark.jobserver.util.{JobserverConfig, SparkJobUtils}

import scala.collection.JavaConverters._
import org.scalatest._

object PythonSessionContextFactorySpec {

  /**
    * Because of this issue https://issues.apache.org/jira/browse/SPARK-10872 ,
    * stopping the Spark Context is not enough to release the lock on the Derby DB.
    * We need to release this lock between instantiations of HiveContext. This method manually
    * removes the lock files, and should only be called after the Spark context has been stopped.
    * If SPARK-10872 gets fixed, then this method should no longer be necessary.
    *
    * This approach wouldn't be suitable in a production scenario, but ok for tests.
    */
  // Note: Issue SPARK-10872 is RESOLVED by now with "Not A Problem"
  private def resetDerby(): Unit = {
    Files.deleteIfExists(Paths.get("/tmp/metastore_db/dbex.lck"))
    Files.deleteIfExists(Paths.get("/tmp/metastore_db/db.lck"))
  }
}

class TestPythonSessionContextFactory extends PythonSessionContextFactory {

  override def makeContext(sparkConf: SparkConf,
                           contextConfig: Config,
                           contextName: String): C = {
    initializeContext(sparkConf, contextConfig, contextName)
  }

  def initializeContext(sparkConf: SparkConf,
                        contextConfig: Config,
                        contextName: String): C = {
    if (! context.isInstanceOf[PythonSessionContextLikeWrapper]) {
      val builder = SparkSession.builder().config(sparkConf.set("spark.yarn.isPython", "true"))
      builder.appName(contextName).master("local")
      builder.config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:myDB;create=true")
      builder.config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      super.setupHiveSupport(contextConfig, builder)
      val spark = builder.getOrCreate()
      for ((k, v) <- SparkJobUtils.getHadoopConfig(contextConfig))
        spark.sparkContext.hadoopConfiguration.set(k, v)
      context = PythonSessionContextLikeWrapper(spark, contextConfig)
    }
    context
  }
}

class PythonSessionContextFactorySpec extends FunSpec with Matchers with BeforeAndAfter
with BeforeAndAfterAll {
  import PythonSparkContextFactorySpec._

  var context: PythonSessionContextLikeWrapper = null
  implicit val system = JavaStreamingSpec.getNewSystem
  lazy val conf = new SparkConf().setMaster("local[*]").setAppName("SubprocessSpec")
  lazy val sc = new SparkContext(conf)

  after {
    if (context != null) {
      context.stop()
    }
    PythonSessionContextFactorySpec.resetDerby()
  }

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  /**
    * resetDerby workaround doesn't work on Windows (file remains locked), so ignore the tests
    * for now
   */
  describe("PythonSessionContextFactorySpec") {
    it("should create PythonSessionContexts", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      context shouldBe an[PythonSessionContextLikeWrapper]
    }

    it("should have a flag for JobManager, that it is python job", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      factory.runsPython shouldBe true
    }

    it("should create JobContainers", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      val result = factory.loadAndValidateJob(Seq("test"), "path.to.Job", DummyJobCache)
      result.isGood should be (true)
      val jobContainer = result.get
      jobContainer shouldBe an[PythonJobContainer[_]]
      jobContainer.getSparkJob should be (
        PythonJob("/tmp/test.egg", "path.to.Job",
          PythonContextFactory.hiveContextImports))
    }

    it("should validate the job if only one file is submitted", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      val jobsLoader = new JobsLoader(10, TestProbe().ref, sc, null)
      val result = factory.loadAndValidateJob(Seq("/tmp/test.egg"), "path.to.Job", jobsLoader)
      result.isGood should be (true)
    }

    it("should raise an error if more than one file submitted to start the job", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      val jobsLoader = new JobsLoader(10, TestProbe().ref, sc, null)
      val result = factory.loadAndValidateJob(Seq("test", "test2"), "path.to.Job", jobsLoader)
      result.isBad should be (true)
    }

    def runSessionTest(factory: TestPythonSessionContextFactory,
                   context: PythonSessionContextLikeWrapper,
                   c: Config,
                   pythonJobName: String = "example_jobs.session_window.SessionWindowJob"): Unit = {
      val loadResult = factory.loadAndValidateJob(
        Seq("sql-average"),
        pythonJobName,
        DummyJobCache)
      loadResult.isGood should be (true)
      val jobContainer = loadResult.get
      val job = jobContainer.getSparkJob
      val jobConfig = ConfigFactory.parseString(
        """
          |input.data = [
          |  ["bob", 20, 1200],
          |  ["jon", 21, 1400],
          |  ["mary", 20, 1300],
          |  ["sue", 21, 1600]
          |]
        """.stripMargin)
      val jobEnv = DummyJobEnvironment("1234", config)
      val jobDataOrProblem = job.validate(context, jobEnv, jobConfig)
      jobDataOrProblem.isGood should be (true)
      val jobData = jobDataOrProblem.get
      val result = job.runJob(context, jobEnv, jobData)
      result should matchPattern {
        case l: java.util.List[_]
          if l.asScala.toSeq.map(_.asInstanceOf[java.util.List[AnyVal]].asScala.toSeq) ==
            Seq(
              Seq("bob", 20, 1),
              Seq("mary", 20, 2),
              Seq("jon", 21, 1),
              Seq("sue", 21, 2)) =>
      }
    }

    it("should return jobs which can be successfully run", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      runSessionTest(factory, context, config)
    }

    it("should successfully run jobs using python3", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      val p3Config = ConfigFactory.parseString(
        s"""
          |python.executable = "${sys.env.getOrElse("PYTHON_EXECUTABLE", "python3")}"
        """.stripMargin).withFallback(config)
      context = factory.makeContext(sparkConf, p3Config, "test-create")
      runSessionTest(factory, context, p3Config)
    }

    it("should throw exception if hive is disabled and hive job is executed") {
      val configWithHiveDisabled = ConfigFactory.parseString(
        s"${JobserverConfig.IS_SPARK_SESSION_HIVE_ENABLED}=false").withFallback(config)
      val factory = new TestPythonSessionContextFactory()
      context = factory.makeContext(sparkConf, configWithHiveDisabled, "test-create")

      val exception = intercept[java.lang.Exception] {
        runSessionTest(factory, context, configWithHiveDisabled,
          "example_jobs.hive_support_job.HiveSupportJob")
      }

      exception.getMessage().contains(
        "Hive support is required to CREATE Hive TABLE (AS SELECT)") should be(true)
      exception.getMessage().contains("check_support") should be(true)
    }
  }
}
