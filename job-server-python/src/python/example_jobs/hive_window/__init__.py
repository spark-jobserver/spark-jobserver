from sparkjobserver.api import SparkJob, build_problems
from pyspark.sql import HiveContext


class HiveWindowJob(SparkJob):

    def validate(self, context, runtime, config):
        problems = []
        job_data = None
        if not isinstance(context, HiveContext):
            problems.append('Expected a HiveContext context')
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
        # Window functions only available on
        # HiveContext so differentiates from a SQLContext job
        query = context.sql("""
          SELECT name, age, RANK() OVER (partition by age order by name)
          FROM people ORDER BY age
        """)
        results = query.collect()
        return [(r[0], r[1], r[2]) for r in results]
