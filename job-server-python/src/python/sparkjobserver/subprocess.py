"""
This module is a runnable program designed to be called from
a JVM process in order to execute a Python Spark-Job-Server job.

It should be executed using a single argument, which is the port
number of the Py4J gateway client which the JVM application should
start before calling this program as a subprocess.

The JVM gateway should include an endpoint method through which
this program can retrieve an object containing information about
the Job to be run. Since Python is not strongly typed, the endpoint
can be any type of JVM object.
The case class `spark.jobserver.python.JobEndpoint`
implements all the methods that this program expects an endpoint to have.
"""

from __future__ import print_function
import sys
from importlib import import_module
from py4j.java_gateway import JavaGateway, java_import, GatewayClient
from pyhocon import ConfigFactory
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from sparkjobserver.api import ValidationProblem, JobEnvironment
import traceback


def exit_with_failure(message, exit_code=1):
    """
    Terminate the process with a specific message and error code
    :param message: The message to write to stderr
    :param exitCode: The exit code with which to terminate
    :return: N/A, the process terminates when this method is called
    """
    print(message, file=sys.stderr)
    sys.exit(exit_code)


def import_class(cls):
    """
    Import a python class where its identity is not known until runtime.
    :param cls: The fully qualified path of the class including module
    prefixes, e.g. sparkjobserver.api.SparkJob
    :return: The constructor for the class, as a function which can be
    called to instantiate an instance.
    """
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

    context_config =\
        ConfigFactory.parse_string(entry_point.contextConfigAsHocon())
    job_id = entry_point.jobId()
    job_env = JobEnvironment(job_id, None, context_config)
    job_config = ConfigFactory.parse_string(entry_point.jobConfigAsHocon())
    job_class = import_class(entry_point.jobClass())
    job = job_class()

    jcontext = entry_point.context()
    jspark_conf = entry_point.sparkConf()
    spark_conf = SparkConf(_jconf=jspark_conf)
    context_class = jcontext.contextType()
    context = None
    if context_class == 'org.apache.spark.api.java.JavaSparkContext':
        context = SparkContext(
                gateway=gateway, jsc=jcontext, conf=spark_conf)
    elif context_class == 'org.apache.spark.sql.SQLContext':
        jsc = gateway.jvm.org.apache.spark.api.java.JavaSparkContext(
                jcontext.sparkContext())
        sc = SparkContext(gateway=gateway, jsc=jsc, conf=spark_conf)
        context = SQLContext(sc, jcontext)
    elif context_class == 'org.apache.spark.sql.hive.HiveContext':
        jsc = gateway.jvm.org.apache.spark.api.java.JavaSparkContext(
                jcontext.sparkContext())
        sc = SparkContext(gateway=gateway, jsc=jsc, conf=spark_conf)
        context = HiveContext(sc, jcontext)
    else:
        customContext = job.build_context(gateway, jcontext, spark_conf)
        if customContext is not None:
            context = customContext
        else:
            exit_with_failure(
                    "Expected JavaSparkContext, SQLContext "
                    "or HiveContext but received %s" % repr(context_class), 2)
    try:
        job_data = job.validate(context, None, job_config)
    except Exception as error:
        exit_with_failure(
            "Error while calling 'validate': %s\n%s" %
            (repr(error), traceback.format_exc()), 3)
    if isinstance(job_data, list) and \
            isinstance(job_data[0], ValidationProblem):
        entry_point.setValidationProblems([p.problem for p in job_data])
        exit_with_failure("Validation problems in job, exiting")
    else:
        try:
            result = job.run_job(context, job_env, job_data)
        except Exception as error:
            exit_with_failure(
                "Error while calling 'run_job': %s\n%s" %
                (repr(error), traceback.format_exc()), 4)
        entry_point.setResult(result)
