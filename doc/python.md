<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Python Support](#python-support)
  - [Setting up Spark Job Server with Python support](#setting-up-spark-job-server-with-python-support)
  - [Writing a Python job](#writing-a-python-job)
    - [Return types](#return-types)
    - [Packaging a job](#packaging-a-job)
  - [Running a job](#running-a-job)
  - [PythonSessionContext](#pythonsessioncontext)
  - [CustomContexts](#customcontexts)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Python Support

Spark Job Server supports Python jobs through a Python specific context factory
`spark.jobserver.python.PythonSessionContextFactory`. See the [Contexts](contexts.md) documentation
for information on contexts.

## Setting up Spark Job Server with Python support

The `PythonSessionContextFactory` class is part of `job-server-extras`, therefore it is the assembly jar of that sub-module
which should be used (this is the default for deployment anyway).

The application config can be configured with paths to be added to the `PYTHONPATH` environment variable when the python
subprocess which runs the jobs is executed. At a minimum, it should include the path of the Job Server API Egg file and
the location of the Spark python libraries (which are most conveniently referenced in relation to `SPARK_HOME`.

By default python jobs are executed by calling an executable named `python`. In some cases, an alternative executable
may be desired, for example to use Python 3 or to use an executable with an absolute path rather than assuming the
executable can be located on the `PATH`.

Python binaries are only supported in the SQL flavour of `JobDAO` therefore it is also necessary to specify this in the config.

A basic config supporting Python might look like:

    spark {
      jobserver {
         jobdao = spark.jobserver.io.JobSqlDAO
      }

      context-settings {
        python {
          paths = [
            ${SPARK_HOME}/python,
            "/home/user/spark-jobserver/job-server-extras/job-server-python/target/python/spark_jobserver_python-0.8.0-py2.7.egg"
          ]

          # The default value in application.conf is "python"
          executable = "python3"
        }
      }
    }

There are two python packages which need to be provided, `py4j` and `pyhocon`. These can either be installed into the
python instance to be used for running the jobs, or added to the list of paths in the config.

In development, you can run a Python ready job server locally with the following `reStart` task:

    job-server-extras/reStart ../config/python-example.conf.template

## Writing a Python job

When developing a Java/Scala job, it is necessary to implement a specific Job Trait in order that the ContextFactory receives
an object of the correct class. However Python does not have the same strong typing so any Python class which implements the methods
expected by the Python bootstrapping code would be sufficient.

The interface to conform to is shown by the `SparkJob` python class:

```python
class SparkJob:

def __init__(self):
    pass

def validate(self, context, runtime, config):
    """
    This method is called by the job server to allow jobs to validate their
    input and reject invalid job requests.
    :param context: the context to be used for the job. Could be a
    SparkContext, SQLContext, HiveContext etc.
    May be reused across jobs.
    :param runtime: the JobEnvironment containing run time information
    pertaining to the job and context.
    :param config: the HOCON config object passed into the job request
    :return: either JobData, which is parsed from config, or a list of
    validation problems.
    """
    raise NotImplementedError("Concrete implementations should override validate")

def run_job(self, context, runtime, data):
    """
    Entry point for the execution of a job
    :param context: the context to be used for the job.
    SparkContext, SQLContext, HiveContext etc.
    May be reused across jobs
    :param runtime: the JobEnvironment containing run time information
    pertaining to the job and context.
    :param data: the JobData returned by the validate method
    :return: the job result OR a list of ValidationProblem objects.
    """
    raise NotImplementedError("Concrete implementations should override run_job")
```

It is possible but not necessary to override this class when providing a conforming job class. When returning a list of
validation problems, it is necessary to return instances of `sparkjobserver.api.ValidationProblem` since otherwise there
is no way to differentiate between validation errors and valid job data. Instances of `ValidationProblem` can be built
from the `build_problems` utility method. Consider the following basic implementation of a Python job:

```python
from sparkjobserver.api import SparkJob, build_problems

class WordCountSparkJob(SparkJob):

    def validate(self, context, runtime, config):
        if config.get('input.strings', None):
            return config.get('input.strings')
        else:
            return build_problems(['config input.strings not found'])

    def run_job(self, context, runtime, data):
        return context._sc.parallelize(data).countByValue()
```

### Return types

Due to job results needing to be converted from Python objects to objects which are serializable by Spray JSON,
only a defined set of return types are supported:

 - Primitive types of boolean, byte, char, short, int, long, float, double and string.
 - Python dicts, which end up as Scala Maps.
 - Python lists which end up as Scala Lists.

The supported collection types of Map and List can be nested, i.e. a return type of Map[String, List[Int]]
would be fine.

### Packaging a job

In order to be able to push a job to the the job server, it must be packaged into a Python Egg file. Similar to a Jar,
this is just a Zip file with a particular internal structure. Eggs can be built using the
[setuptools](https://setuptools.readthedocs.io/en/latest/) library.

In the most basic setup, a job ready for packaging would be structured as:

- **setup.py** Contains the configuration for packaging the job
- **my_job_package** A directory, the name of which is the name of the module containing your job(s)
- **\_\_init\_\_.py** A file inside the module directory, which contains the python implementation of one or more job classes.

`setup.py` would contain something like:

```python
    from setuptools import setup,

    setup(
        name='my_job_package',
        packages=['my_job_package']
    )
```

Then, running `python setup.py bdist_egg` will create a file `dist/my_job_package-0.0.0-py2.7.egg`.

## Running a job

If Spark Job Server is running with Python support, A Python context can be started with, for example:

    curl -X POST "localhost:8090/contexts/py-context?context-factory=spark.jobserver.python.PythonSessionContextFactory"

Whereas Java and Scala jobs are packaged as Jar files, Python jobs need to be packaged as `Egg` files. A set of example jobs
can be build using the `job-server-python/` sbt task `job-server-python/buildPyExamples`. this builds an examples Egg
in `job-server-python/target/python` so we could push this to the server as a job binary:

    curl --data-binary @dist/my_job_package-0.0.0-py2.7.egg \
    -H 'Content-Type: application/python-archive' localhost:8090/binaries/my_py_job

Then, running a Python job is similar to running other job types:

    curl -d 'input.strings = ["a", "b", "a", "b" ]' \
    "localhost:8090/jobs?appName=my_py_job&classPath=my_job_package.WordCountSparkJob&context=py-context"

    curl "localhost:8090/jobs/<job-id>"

Note: The SparkJobServer takes care of pushing the Egg file you uploaded to Spark as part of the job submission, ensuring that all workers have access to the full contents of the Egg file.  

## PythonSessionContext

Python session context provides full access to Spark Session including access to the underlaying Spark Context.  
The previously available  `SQLContext` and `HiveContext` Context Factory are no longer supported. 


Simply launch a context using
`spark.jobserver.python.PythonSessionContextFactory` For example:

    curl -X POST "localhost:8090/contexts/pysql-context?context-factory=spark.jobserver.python.PythonSessionContextFactory"

When implementing the Python job, you can simply assume that the `context` argument to `validate` and `run_job`
is of the appropriate type. Due to dynamic typing in Python, this is not enforced in the method definitions. For example:

```python
class SQLAverageJob(SparkJob):

    def validate(self, context, runtime, config):
        problems = []
        job_data = None
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
        query = context.sql("SELECT age, AVG(salary) from people GROUP BY age ORDER BY age")
        results = query.collect()
        return [ (r[0], r[1]) for r in results]
```

The above job implementation checks during the `validate` stage that the `context` object is of the correct type.
Then in `run_job` dataframe operations are used, which exist on `SessionContext`.

This job is one of the examples so running the sbt task `job-server-python/buildPyExamples` and uploading the resulting
Egg makes this job available:

    curl --data-binary @job-server-python/target/python/sjs_python_examples-0.8.0-py2.7.egg \
    -H 'Content-Type: application/python-archive' localhost:8090/binaries/example_jobs

The input to the job can be provided as a conf file, in this example `sqlinput.conf`, with the contents:

    input.data = [
      ["bob", 20, 1200],
      ["jon", 21, 1400],
      ["mary", 20, 1300],
      ["sue", 21, 1600]
    ]

Then we can submit the `SessionContext` based job:

    curl -d @sqlinput.conf \
    "localhost:8090/jobs?appName=example_jobs&classPath=example_jobs.sql_average.SQLAverageJob&context=pysql-context"

    curl "localhost:8090/jobs/<job-id>"

When complete, we get output such as:

    {
      "duration": "4.685 secs",
      "classPath": "example_jobs.sql_average.SQLAverageJob",
      "startTime": "2016-08-01T15:15:52.250+01:00",
      "context": "pysql-context",
      "result": [[20, 1250.0], [21, 1500.0]],
      "status": "FINISHED",
      "jobId": "05346d12-a84b-40f7-a88d-15765bdd23a4"
    }

## CustomContexts

The Python support can support arbitrary context types as long as they are based on an implementation of the
`spark.jobserver.python.PythonContextFactory` Trait. As well as implementing a version of this Trait which yields
contexts of your custom type, your Python jobs which use this context must implement an additional method,
`build_context(self, gateway, jvmContext, sparkConf)`, which returns the Python equivalent of the JVM Context object.
For a simple example, see `CustomContextJob` in the `job-server-python` sub-module.
