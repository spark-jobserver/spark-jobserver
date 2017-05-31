package spark.jobserver.python

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, Matchers, FunSpec}
import spark.jobserver.WindowsIgnore
import scala.collection.JavaConverters._

class PythonSQLContextFactorySpec extends FunSpec with Matchers with BeforeAndAfter{

  import PythonSparkContextFactorySpec._

  var context: SQLContext with PythonContextLike = null

  after {
    if(context != null) {
      context.stop()
    }
  }

  describe("PythonSQLContextFactory") {

    it("should create PythonSQLContexts") {
      val factory = new PythonSQLContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      context shouldBe an[SQLContext with PythonContextLike]
    }

    it("should create JobContainers") {
      val factory = new PythonSQLContextFactory()
      val result = factory.loadAndValidateJob("test", DateTime.now(), "path.to.Job", DummyJobCache)
      result.isGood should be (true)
      val jobContainer = result.get
      jobContainer shouldBe an[PythonJobContainer[_]]
      jobContainer.getSparkJob should be (
        PythonJob("/tmp/test.egg", "path.to.Job",
          PythonContextFactory.sqlContextImports))
    }

    def runSqlTest(factory: PythonSQLContextFactory,
                   context: SQLContext with PythonContextLike,
                   c:Config): Unit = {
      val loadResult = factory.loadAndValidateJob(
        "sql-average",
        DateTime.now(),
        "example_jobs.sql_average.SQLAverageJob",
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
            Seq(Seq(20, 1250.0), Seq(21, 1500.0)) =>
      }
    }

    it("should return jobs which can be successfully run") {
      val factory = new PythonSQLContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      runSqlTest(factory, context, config)
    }

    it("should successfully run jobs using python3", WindowsIgnore) {
      val factory = new PythonSQLContextFactory()
      val p3Config = ConfigFactory.parseString(
        """
          |python.executable = "python3"
        """.stripMargin).withFallback(config)
      context = factory.makeContext(sparkConf, p3Config, "test-create")
      runSqlTest(factory, context, p3Config)
    }
  }
}
