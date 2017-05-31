from sparkjobserver.api import SparkJob, build_problems
from pyspark.sql import SQLContext


class SQLAverageJob(SparkJob):

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
        df = context.createDataFrame(rdd, ['name', 'age', 'salary'])
        df.registerTempTable('people')
        query = context.sql("SELECT age, AVG(salary) "
                            "from people GROUP BY age ORDER BY age")
        results = query.collect()
        return [(r[0], r[1]) for r in results]
