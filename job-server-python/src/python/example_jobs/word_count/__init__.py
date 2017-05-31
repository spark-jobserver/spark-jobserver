from sparkjobserver.api import SparkJob, build_problems


class WordCountSparkJob(SparkJob):

    def validate(self, context, runtime, config):
        if config.get('input.strings', None):
            return config.get('input.strings')
        else:
            return build_problems(['config input.strings not found'])

    def run_job(self, context, runtime, data):
        return context.parallelize(data).countByValue()


class FailingSparkJob(SparkJob):
    """
    Simple example of a SparkContext job that fails
    with an exception for use in tests
    """

    def validate(self, context, runtime, config):
        if config.get('input.strings', None):
            return config.get('input.strings')
        else:
            return build_problems(['config input.strings not found'])

    def run_job(self, context, runtime, data):
        raise ValueError('Deliberate failure')
