from sparkjobserver.api import SparkJob, build_problems
from pyspark import SparkContext


class CustomContext(SparkContext):

    def __init__(self, gateway, customContext, sparkConf):
        self.jcustomContext = customContext
        SparkContext.__init__(
                self, gateway=gateway, jsc=customContext, conf=sparkConf)

    def customMethod(self):
        return self.jcustomContext.customMethod()


class CustomContextJob(SparkJob):

    def validate(self, context, runtime, config):
        if config.get('input.strings', None):
            return config.get('input.strings')
        else:
            return build_problems(['config input.strings not found'])

    def run_job(self, context, runtime, data):
        count = context.parallelize(data).count()
        return context.customMethod() + " " + str(count)

    def build_context(self, gateway, jvmContext, sparkConf):
        return CustomContext(gateway, jvmContext, sparkConf)
