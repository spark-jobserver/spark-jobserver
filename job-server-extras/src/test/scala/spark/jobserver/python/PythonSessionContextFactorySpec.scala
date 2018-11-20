package spark.jobserver.python

import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FunSpec, Ignore, Matchers}
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import spark.jobserver.WindowsIgnore
import spark.jobserver.util.SparkJobUtils

import scala.collection.JavaConverters._


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

class TestPythonSessionContextFactory extends PythonContextFactory {

  override type C = PythonSessionContextLikeWrapper
  var context : PythonSessionContextLikeWrapper = _

  override def py4JImports: Seq[String] =
    PythonContextFactory.hiveContextImports

  override def doMakeContext(sc: SparkContext,
                             contextConfig: Config,
                             contextName: String): C = {
    context
  }

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
      try {
        builder.enableHiveSupport()
      } catch {
        case e: IllegalArgumentException =>
          println(s"Hive support not enabled - ${e.toString}")
      }
      val spark = builder.getOrCreate()
      for ((k, v) <- SparkJobUtils.getHadoopConfig(contextConfig))
        spark.sparkContext.hadoopConfiguration.set(k, v)
      context = PythonSessionContextLikeWrapper(spark, contextConfig)
    }
    context
  }
}


class PythonSessionContextFactorySpec extends FunSpec with Matchers with BeforeAndAfter {
  import PythonSparkContextFactorySpec._

  var context: PythonSessionContextLikeWrapper = _

  after {
    if (context != null) {
      context.stop()
    }
    PythonSessionContextFactorySpec.resetDerby()
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

    it("should create JobContainers", WindowsIgnore) {
      val factory = new TestPythonSessionContextFactory()
      val result = factory.loadAndValidateJob("test", DateTime.now(), "path.to.Job", DummyJobCache)
      result.isGood should be (true)
      val jobContainer = result.get
      jobContainer shouldBe an[PythonJobContainer[_]]
      jobContainer.getSparkJob should be (
        PythonJob("/tmp/test.egg", "path.to.Job",
          PythonContextFactory.hiveContextImports))
    }

    def runSessionTest(factory: TestPythonSessionContextFactory,
                   context: PythonSessionContextLikeWrapper,
                   c: Config): Unit = {
      val loadResult = factory.loadAndValidateJob(
        "sql-average",
        DateTime.now(),
        "example_jobs.session_window.SessionWindowJob",
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
        """
          |python.executable = "python3"
        """.stripMargin).withFallback(config)
      context = factory.makeContext(sparkConf, p3Config, "test-create")
      runSessionTest(factory, context, p3Config)
    }
  }
}
