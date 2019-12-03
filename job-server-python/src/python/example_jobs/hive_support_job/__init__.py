from sparkjobserver.api import SparkJob, build_problems


class HiveSupportJob(SparkJob):
    def validate(self, context, runtime, config):
        return None

    def run_job(self, context, runtime, data):
        context.sql('CREATE TABLE IF NOT EXISTS check_support (key INT, value STRING) USING hive')
