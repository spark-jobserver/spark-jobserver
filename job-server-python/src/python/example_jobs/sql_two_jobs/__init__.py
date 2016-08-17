from sparkjobserver.api import SparkJob, build_problems
from pyspark.sql import SQLContext


class Job1(SparkJob):

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
        # defining the temp table in terms of data written to disk,
        # since basing it off a dataframe made up of parallelizing
        # an in-process list within a Python job causes
        # problems when a different job, constituting a different
        # Python process, tries to use that dataframe.
        context.createDataFrame(rdd, ['name', 'age', 'salary']).\
            write.save("/tmp/people.parquet", mode='overwrite')
        context.read.load('/tmp/people.parquet').\
            registerTempTable('people_table')
        return "done"


class Job2(SparkJob):

    def validate(self, context, runtime, config):
        problems = []
        job_data = None
        if not isinstance(context, SQLContext):
            problems.append('Expected a SQL context')
        if 'people_table' in context.tableNames():
            job_data = ""
        else:
            problems.append("expect 'people_table' table to "
                            "have been created by earlier job")
        if len(problems) == 0:
            return job_data
        else:
            return build_problems(problems)

    def run_job(self, context, runtime, data):
        query = context.sql("""
        SELECT age, AVG(salary)
        FROM people_table GROUP BY age ORDER BY age""")
        results = query.collect()
        return results
