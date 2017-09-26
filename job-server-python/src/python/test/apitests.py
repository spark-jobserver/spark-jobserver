import unittest
from pyhocon import ConfigFactory
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from sparkjobserver.api import SparkJob, build_problems, ValidationProblem
from py4j.java_gateway import java_import


class WordCountSparkJob(SparkJob):
    """
    Simple example of a SparkContext job for use in tests
    """

    def validate(self, context, runtime, config):
        if config.get('input.strings', None):
            return config.get('input.strings')
        else:
            return build_problems(['config input.strings not found'])

    def run_job(self, context, runtime, data):
        return context.parallelize(data).countByValue()


class SQLJob(SparkJob):
    """
    Simple example of a Spark SQL job for use in tests.
    Could be either SQLContext or HiveContext
    """

    def validate(self, context, runtime, config):
        problems = []
        job_data = None
        if not isinstance(context, SQLContext):
            problems.append('Expected a SQL context')
        if config.get('input.data', None):
            job_data = config.get('input.data')
        else:
            problems.append('config input.data not found')
        if len(problems) == 0:
            return job_data
        else:
            return build_problems(problems)

    def run_job(self, context, runtime, data):
        rdd = context._sc.parallelize(data)
        df = context.createDataFrame(data, ['name', 'age', 'salary'])
        df.registerTempTable('people')
        query = context.sql("""
            SELECT age, AVG(salary)
            from people GROUP BY age ORDER BY age""")
        results = query.collect()
        return [(r[0], r[1]) for r in results]


class TestSJSApi(unittest.TestCase):

    def setUp(self):
        conf = SparkConf().setAppName('test').setMaster('local[*]')
        self.sc = SparkContext(conf=conf)
        self.jvm = self.sc._gateway.jvm
        java_import(self.jvm, "org.apache.spark.sql.*")

    def tearDown(self):
        self.sc.stop()

    def test_validation_failure(self):
        job = WordCountSparkJob()
        result = job.validate(self.sc, None, ConfigFactory.parse_string(""))
        self.assertTrue(isinstance(result, list))
        self.assertEqual(1, len(result))
        self.assertTrue(isinstance(result[0], ValidationProblem))

    def test_validation_success(self):
        job = WordCountSparkJob()
        result = job.validate(
                self.sc,
                None,
                ConfigFactory.parse_string('input.strings = ["a", "a", "b"]'))
        self.assertEqual(result, ['a', 'a', 'b'])

    def test_run_job(self):
        job = WordCountSparkJob()
        jobData = job.validate(
                None,
                self.sc,
                ConfigFactory.parse_string('input.strings = ["a", "a", "b"]'))
        result = job.run_job(self.sc, None, jobData)
        self.assertEqual(result['a'], 2)
        self.assertEqual(result['b'], 1)

    def test_sql_job_validation_failure(self):
        job = SQLJob()
        result = job.validate(self.sc, None, ConfigFactory.parse_string(""))
        self.assertTrue(isinstance(result, list))
        self.assertEqual(2, len(result))
        self.assertTrue(isinstance(result[0], ValidationProblem))
        self.assertTrue(isinstance(result[1], ValidationProblem))
        self.assertEqual('Expected a SQL context', result[0].problem)
        self.assertEqual('config input.data not found', result[1].problem)

    def test_run_sql_job(self):
        job = SQLJob()
        sqlContext = SQLContext(self.sc)
        config = ConfigFactory.parse_string("""
          input.data = [
            ['bob', 20, 1200],
            ['jon', 21, 1400],
            ['mary', 20, 1300],
            ['sue, 21, 1600]
          ]
        """)
        jobData = job.validate(sqlContext, None, config)
        result = job.run_job(sqlContext, None, jobData)
        self.assertEqual([(20, 1250), (21, 1500)], result)

    def test_run_hive_job(self):
        job = SQLJob()
        sqlContext = HiveContext(self.sc)
        config = ConfigFactory.parse_string("""
          input.data = [
            ['bob', 20, 1200],
            ['jon', 21, 1400],
            ['mary', 20, 1300],
            ['sue, 21, 1600]
          ]
        """)
        jobData = job.validate(sqlContext, None, config)
        result = job.run_job(sqlContext, None, jobData)
        self.assertEqual([(20, 1250), (21, 1500)], result)

if __name__ == "__main__":
    unittest.main()
