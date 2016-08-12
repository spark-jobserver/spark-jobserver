from sparkjobserver.api import SparkJob, build_problems


class FailingRunJob(SparkJob):

    def validate(self, context, runtime, config):
        return "fine"

    def run_job(self, context, runtime, data):
        raise Exception("Deliberate failure")


class FailingValidateJob(SparkJob):

    def validate(self, context, runtime, config):
        raise Exception("Deliberate failure")

    def run_job(self, context, runtime, data):
        pass
