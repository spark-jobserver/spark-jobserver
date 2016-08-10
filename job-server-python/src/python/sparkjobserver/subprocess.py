from __future__ import print_function

import sys
from importlib import import_module
from py4j.java_gateway import JavaGateway, java_import, GatewayClient
from pyhocon import ConfigFactory
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from sparkjobserver.api import ValidationProblem

def exit_with_failure(message, exitCode = 1):
    print(message, file = sys.stderr)
    sys.exit(exitCode)

def import_class(cls):
    (module_name, class_name) = cls.rsplit('.', 1)
    module = import_module(module_name)
    c = getattr(module, class_name)
    return c

if __name__ == "__main__":
    port = int(sys.argv[1])
    gateway = JavaGateway(GatewayClient(port=port), auto_convert=True)
    entry_point = gateway.entry_point
    imports = entry_point.getPy4JImports()
    for i in imports:
        java_import(gateway.jvm, i)

    jobConfig = ConfigFactory.parse_string(entry_point.jobConfigAsHocon())
    jobClass = import_class(entry_point.jobClass())
    job = jobClass()

    jcontext = entry_point.context()
    jsparkConf = entry_point.sparkConf()
    sparkConf = SparkConf(_jconf = jsparkConf)
    contextClass = jcontext.contextType()
    context = None
    if contextClass == 'org.apache.spark.api.java.JavaSparkContext':
        context = SparkContext(gateway = gateway, jsc = jcontext, conf = sparkConf)
    elif contextClass == 'org.apache.spark.sql.SQLContext':
        jsc = gateway.jvm.org.apache.spark.api.java.JavaSparkContext(jcontext.sparkContext())
        sc = SparkContext(gateway = gateway, jsc = jsc, conf = sparkConf)
        context = SQLContext(sc, jcontext)
    elif contextClass == 'org.apache.spark.sql.hive.HiveContext':
        jsc = gateway.jvm.org.apache.spark.api.java.JavaSparkContext(jcontext.sparkContext())
        sc = SparkContext(gateway = gateway, jsc = jsc, conf = sparkConf)
        context = HiveContext(sc, jcontext)
    else:
        customContext = job.build_context(gateway, jcontext, sparkConf)
        if customContext is not None:
            context = customContext
        else:
            exit_with_failure("Expected JavaSparkContext, SQLContext or HiveContext but received "+repr(contextClass), 2)
    try:
        jobData = job.validate(context, None, jobConfig)
    except Exception as error:
        exit_with_failure("Error while calling 'validate'" + repr(error), 3)
    if isinstance(jobData, list) and isinstance(jobData[0], ValidationProblem):
        entry_point.setValidationProblems([p.problem for p in jobData])
        exit_with_failure("Validation problems in job, exiting")
    else:
        try:
            result = job.run_job(context, None, jobData)
        except Exception as error:
            exit_with_failure("Error while calling 'run_job'" + repr(error), 4)
        entry_point.setResult(result)
