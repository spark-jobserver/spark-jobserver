package spark.jobserver.python

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, Matchers, FunSpec}

import java.nio.file.Files
import java.nio.file.Paths
import spark.jobserver.WindowsIgnore
import scala.collection.JavaConverters._


object PythonHiveContextFactorySpec {

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

class PythonHiveContextFactorySpec extends FunSpec with Matchers with BeforeAndAfter {

  import PythonSparkContextFactorySpec._

  var context: HiveContext with PythonContextLike = null

  after {
    if(context != null) {
      context.stop()
    }
    PythonHiveContextFactorySpec.resetDerby()
  }

  /**
    * resetDerby workaround doesn't work on Windows (file remains locked), so ignore the tests
    * for now
   */
  describe("PythonHiveContextFactory") {
    it("should create PythonHiveContexts", WindowsIgnore) {
      val factory = new PythonHiveContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      context shouldBe an[HiveContext with PythonContextLike]
    }

    it("should create JobContainers", WindowsIgnore) {
      val factory = new PythonHiveContextFactory()
      val result = factory.loadAndValidateJob("test", DateTime.now(), "path.to.Job", DummyJobCache)
      result.isGood should be (true)
      val jobContainer = result.get
      jobContainer shouldBe an[PythonJobContainer[_]]
      jobContainer.getSparkJob should be (
        PythonJob("/tmp/test.egg", "path.to.Job",
          PythonContextFactory.hiveContextImports))
    }

    def runHiveTest(factory: PythonHiveContextFactory,
                   context: HiveContext with PythonContextLike,
                   c:Config): Unit = {
      val loadResult = factory.loadAndValidateJob(
        "sql-average",
        DateTime.now(),
        "example_jobs.hive_window.HiveWindowJob",
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
      val factory = new PythonHiveContextFactory()
      context = factory.makeContext(sparkConf, config, "test-create")
      runHiveTest(factory, context, config)
    }

    it("should successfully run jobs using python3", WindowsIgnore) {
      val factory = new PythonHiveContextFactory()
      val p3Config = ConfigFactory.parseString(
        """
          |python.executable = "python3"
        """.stripMargin).withFallback(config)
      context = factory.makeContext(sparkConf, p3Config, "test-create")
      runHiveTest(factory, context, p3Config)
    }
  }
}
