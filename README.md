[![Build Status](https://travis-ci.org/spark-jobserver/spark-jobserver.svg?branch=master)](https://travis-ci.org/spark-jobserver/spark-jobserver) [![Coverage](https://img.shields.io/codecov/c/github/spark-jobserver/spark-jobserver/master.svg)](https://codecov.io/gh/spark-jobserver/spark-jobserver/branch/master)

[![Join the chat at https://gitter.im/spark-jobserver/spark-jobserver](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/spark-jobserver/spark-jobserver?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

spark-jobserver provides a RESTful interface for submitting and managing [Apache Spark](http://spark-project.org) jobs, jars, and job contexts.
This repo contains the complete Spark job server project, including unit tests and deploy scripts.
It was originally started at [Ooyala](http://www.ooyala.com), but this is now the main development repo.

Other useful links: [Troubleshooting](doc/troubleshooting.md), [cluster](doc/cluster.md), [YARN client](doc/yarn.md), [YARN on EMR](doc/EMR.md), [Mesos](doc/mesos.md), [JMX tips](doc/jmx.md).

Also see [Chinese docs / 中文](doc/chinese/job-server.md).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Users](#users)
- [Features](#features)
- [Version Information](#version-information)
- [Getting Started with Spark Job Server](#getting-started-with-spark-job-server)
- [Development mode](#development-mode)
  - [WordCountExample walk-through](#wordcountexample-walk-through)
    - [Package Jar - Send to Server](#package-jar---send-to-server)
    - [Ad-hoc Mode - Single, Unrelated Jobs (Transient Context)](#ad-hoc-mode---single-unrelated-jobs-transient-context)
    - [Persistent Context Mode - Faster & Required for Related Jobs](#persistent-context-mode---faster--required-for-related-jobs)
  - [Debug mode](#debug-mode)
- [Create a Job Server Project](#create-a-job-server-project)
  - [Creating a project from scratch using giter8 template](#creating-a-project-from-scratch-using-giter8-template)
  - [Creating a project manually assuming that you already have sbt project structure](#creating-a-project-manually-assuming-that-you-already-have-sbt-project-structure)
  - [NEW SparkJob API](#new-sparkjob-api)
  - [NEW SparkJob API with Spark v2.1](#new-sparkjob-api-with-spark-v21)
  - [Dependency jars](#dependency-jars)
  - [Named Objects](#named-objects)
    - [Using Named RDDs](#using-named-rdds)
    - [Using Named Objects](#using-named-objects)
  - [HTTPS / SSL Configuration](#https--ssl-configuration)
    - [Server authentication](#server-authentication)
    - [Client authentication](#client-authentication)
  - [Basic authentication](#basic-authentication)
- [Deployment](#deployment)
  - [Manual steps](#manual-steps)
  - [Context per JVM](#context-per-jvm)
  - [Configuring Spark Jobserver backend](#configuring-spark-jobserver-backend)
    - [Configuring Spark Jobserver H2 Database backend](#configuring-spark-jobserver-h2-database-backend)
    - [Configuring Spark Jobserver PostgreSQL Database backend](#configuring-spark-jobserver-postgresql-database-backend)
    - [Configuring Spark Jobserver MySQL Database backend](#configuring-spark-jobserver-mysql-database-backend)
    - [Configuring Spark Jobserver Zookeeper + HDFS Database backend](#configuring-spark-jobserver-zookeeper--hdfs-database-backend)
    - [Configuring Spark Jobserver Cassandra backend](#configuring-spark-jobserver-cassandra-backend)
  - [HA Deployment (beta)](#ha-deployment-beta)
  - [Chef](#chef)
- [Architecture](#architecture)
- [API](#api)
  - [Binaries](#binaries)
  - [Contexts](#contexts)
  - [Jobs](#jobs)
  - [Data](#data)
    - [Data API Example](#data-api-example)
  - [Context configuration](#context-configuration)
  - [Other configuration settings](#other-configuration-settings)
  - [Job Result Serialization](#job-result-serialization)
  - [HTTP Override](#http-override)
- [Clients](#clients)
- [Contribution and Development](#contribution-and-development)
  - [Publishing packages](#publishing-packages)
- [Contact](#contact)
- [License](#license)
- [TODO](#todo)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Users

(Please add yourself to this list!)

Spark Job Server is included in Datastax Enterprise!

- [Ooyala](http://www.ooyala.com)
- [Netflix](http://www.netflix.com)
- [Avenida.com](http://www.avenida.com)
- GumGum
- Fuse Elements
- Frontline Solvers
- [Aruba Networks](http://www.arubanetworks.com/)
- [Zed Worldwide](http://www.zed.com)
- [KNIME](https://www.knime.org/)
- [Azavea](http://azavea.com)
- [Maana](http://maana.io/)
- [Newsweaver](https://www.newsweaver.com)
- [Instaclustr](http://www.instaclustr.com)
- [SnappyData](http://www.snappydata.io)
- [Linkfluence](http://www.linkfluence.com)
- [Smartsct](http://www.smartsct.com)
- [Datadog](https://www.datadoghq.com/)
- [Planalytics](http://www.planalytics.com)
- [Target](http://www.target.com/)
- [Branch](http://branch.io)

## Features

- *"Spark as a Service"*: Simple REST interface (including HTTPS) for all aspects of job, context management
- Support for Spark SQL, Hive, Streaming Contexts/jobs and custom job contexts!  See [Contexts](doc/contexts.md).
- [Python](doc/python.md), Scala, and [Java](doc/javaapi.md) (see [TestJob.java](https://github.com/spark-jobserver/spark-jobserver/blob/master/job-server-api/src/main/java/spark/jobserver/api/TestJob.java)) support
- LDAP Auth support via Apache Shiro integration
- Separate JVM per SparkContext for isolation (EXPERIMENTAL)
- Supports sub-second low-latency jobs via long-running job contexts
- Start and stop job contexts for RDD sharing and low-latency jobs; change resources on restart
- Kill running jobs via stop context and delete job
- Separate jar uploading step for faster job startup
- Asynchronous and synchronous job API.  Synchronous API is great for low latency jobs!
- Works with Standalone Spark as well on [cluster](doc/cluster.md), [Mesos](doc/mesos.md), YARN [client](doc/yarn.md) and [on EMR](doc/EMR.md))
- Job and jar info is persisted via a pluggable DAO interface
- Named Objects (such as RDDs or DataFrames) to cache and retrieve RDDs or DataFrames by name, improving object sharing and reuse among jobs.
- Supports Scala 2.10 and 2.11
- Support for supervise mode of Spark (EXPERIMENTAL)
- Possible to be deployed in an [HA setup](#ha-deployment-beta) of multiple jobservers (beta)

## Version Information

| Version     | Spark Version |
|-------------|---------------|
| 0.3.1       | 0.9.1         |
| 0.4.0       | 1.0.2         |
| 0.4.1       | 1.1.0         |
| 0.5.0       | 1.2.0         |
| 0.5.1       | 1.3.0         |
| 0.5.2       | 1.3.1         |
| 0.6.0       | 1.4.1         |
| 0.6.1       | 1.5.2         |
| 0.6.2       | 1.6.1         |
| 0.7.0       | 1.6.2         |
| 0.8.0       | 2.2.0    |
| 0.9.0-SNAPSHOT | 2.3.2 |

For release notes, look in the `notes/` directory.

If you need non-released jars, please visit [Jitpack](https://jitpack.io) - they provide non-release jar builds for any Git repo.  :)

## Getting Started with Spark Job Server

The easiest way to get started is to try the [Docker container](doc/docker.md) which prepackages a Spark distribution with the job server and lets you start and deploy it.

Alternatives:

* Build and run Job Server in local [development mode](#development-mode) within SBT.  NOTE:  This does NOT work for YARN, and in fact is only recommended with `spark.master` set to `local[*]`.  Please deploy if you want to try with YARN or other real cluster.
* Deploy job server to a cluster.  There are two alternatives (see the [deployment section](#deployment)):
  - `server_deploy.sh`  deploys job server to a directory on a remote host.
  - `server_package.sh` deploys job server to a local directory, from which you can deploy the directory, or create a .tar.gz for Mesos or YARN deployment.
* EC2 Deploy scripts - follow the instructions in [EC2](doc/EC2.md) to spin up a Spark cluster with job server and an example application.
* EMR Deploy instruction - follow the instruction in [EMR](doc/EMR.md)

NOTE: Spark Job Server can optionally run `SparkContext`s in their own, forked JVM process when the config option `spark.jobserver.context-per-jvm` is set to `true`.  This option does not currently work for SBT/local dev mode. See [Deployment](#deployment) section for more info.

## Development mode

The example walk-through below shows you how to use the job server with an included example job, by running the job server in local development mode in SBT.  This is not an example of usage in production.

You need to have [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html) installed.

To set the current version, do something like this:

    export VER=`sbt version | tail -1 | cut -f2`

From SBT shell, simply type "reStart".  This uses a default configuration file.  An optional argument is a
path to an alternative config file.  You can also specify JVM parameters after "---".  Including all the
options looks like this:

    job-server-extras/reStart /path/to/my.conf --- -Xmx8g

Note that reStart (SBT Revolver) forks the job server in a separate process.  If you make a code change, simply
type reStart again at the SBT shell prompt, it will compile your changes and restart the jobserver.  It enables
very fast turnaround cycles.

**NOTE2**: You cannot do `sbt reStart` from the OS shell.  SBT will start job server and immediately kill it.

For example jobs see the job-server-tests/ project / folder.

When you use `reStart`, the log file goes to `job-server/job-server-local.log`.  There is also an environment variable
EXTRA_JAR for adding a jar to the classpath.

### WordCountExample walk-through

#### Package Jar - Send to Server
First, to package the test jar containing the WordCountExample: `sbt job-server-tests/package`.
Then go ahead and start the job server using the instructions above.

Let's upload the jar:

    curl -X POST localhost:8090/binaries/test -H "Content-Type: application/java-archive" --data-binary @job-server-tests/target/scala-2.10/job-server-tests-$VER.jar
    OK⏎

#### Ad-hoc Mode - Single, Unrelated Jobs (Transient Context)
The above jar is uploaded as app `test`.  Next, let's start an ad-hoc word count job, meaning that the job
server will create its own SparkContext, and return a job ID for subsequent querying:

    curl -d "input.string = a b c a b see" "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample"
    {
      "duration": "Job not done yet",
      "classPath": "spark.jobserver.WordCountExample",
      "startTime": "2016-06-19T16:27:12.196+05:30",
      "context": "b7ea0eb5-spark.jobserver.WordCountExample",
      "status": "STARTED",
      "jobId": "5453779a-f004-45fc-a11d-a39dae0f9bf4"
    }⏎

NOTE: If you want to feed in a text file config and POST using curl, you want the `--data-binary` option, otherwise
curl will munge your line separator chars.  Like:

    curl --data-binary @my-job-config.json "localhost:8090/jobs?appNam=..."

NOTE2: If you want to send in UTF-8 chars, make sure you pass in a proper header to CURL for the encoding, otherwise it may assume an encoding which is not what you expect.

From this point, you could asynchronously query the status and results:

    curl localhost:8090/jobs/5453779a-f004-45fc-a11d-a39dae0f9bf4
    {
      "duration": "6.341 secs",
      "classPath": "spark.jobserver.WordCountExample",
      "startTime": "2015-10-16T03:17:03.127Z",
      "context": "b7ea0eb5-spark.jobserver.WordCountExample",
      "result": {
        "a": 2,
        "b": 2,
        "c": 1,
        "see": 1
      },
      "status": "FINISHED",
      "jobId": "5453779a-f004-45fc-a11d-a39dae0f9bf4"
    }⏎

Note that you could append `&sync=true` when you POST to /jobs to get the results back in one request, but for
real clusters and most jobs this may be too slow.

You can also append `&timeout=XX` to extend the request timeout for `sync=true` requests.

#### Persistent Context Mode - Faster & Required for Related Jobs
Another way of running this job is in a pre-created context.  Start a new context:

    curl -d "" "localhost:8090/contexts/test-context?num-cpu-cores=4&memory-per-node=512m"
    OK⏎

You can verify that the context has been created:

    curl localhost:8090/contexts
    ["test-context"]⏎

Now let's run the job in the context and get the results back right away:

    curl -d "input.string = a b c a b see" "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample&context=test-context&sync=true"
    {
      "result": {
        "a": 2,
        "b": 2,
        "c": 1,
        "see": 1
      }
    }⏎

Note the addition of `context=` and `sync=true`.

### Debug mode
Spark job server is started using SBT Revolver (which forks a new JVM), so debugging directly in an IDE is not feasible.
To enable debugging, the Spark job server should be started from the SBT shell with the following Java options :
```bash
job-server-extras/reStart /absolute/path/to/your/dev.conf --- -Xdebug -Xrunjdwp:transport=dt_socket,address=15000,server=y,suspend=y
```
The above command starts a remote debugging server on port 15000. The Spark job server is not started until a debugging client
(Intellij, Eclipse, telnet, ...) connects to the exposed port.

In your IDE you just have to start a Remote debugging debug job and use the above defined port. Once the client connects to the debugging server the Spark job server is started and you can start adding breakpoints and debugging requests.

Note that you might need to adjust some server parameters to avoid short Spary/Akka/Spark timeouts, in your `dev.conf` add the following values :
```bash
spark {
  jobserver {
    # Dev debug timeouts
    context-creation-timeout = 1000000 s
    yarn-context-creation-timeout = 1000000 s
    default-sync-timeout = 1000000 s
  }

  context-settings {
    # Dev debug timeout
    context-init-timeout = 1000000 s
  }
}
spray.can.server {
      # Debug timeouts
      idle-timeout = infinite
      request-timeout = infinite
}
```

Additionally, you might have to increase the Akka Timeouts by adding the following query parameter `timeout=1000000` in your HTTP requests :
```bash
curl -d "input.string = a b c a b see" "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample&sync=true&timeout=100000"
```

## Create a Job Server Project
### Creating a project from scratch using giter8 template

There is a giter8 template available at https://github.com/spark-jobserver/spark-jobserver.g8

    $ sbt new spark-jobserver/spark-jobserver.g8

Answer the questions to generate a project structure for you. This contains Word Count example spark job using both old API and new one.

    $ cd /path/to/project/directory
    $ sbt package

Now you could remove example application and start adding your one.

### Creating a project manually assuming that you already have sbt project structure
In your `build.sbt`, add this to use the job server jar:

        resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

        libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.8.0" % "provided"

If a SQL or Hive job/context is desired, you also want to pull in `job-server-extras`:

    libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.8.0" % "provided"

For most use cases it's better to have the dependencies be "provided" because you don't want SBT assembly to include the whole job server jar.

To create a job that can be submitted through the job server, the job must implement the `SparkJob` trait.
Your job will look like:
```scala
object SampleJob extends SparkJob {
    override def runJob(sc: SparkContext, jobConfig: Config): Any = ???
    override def validate(sc: SparkContext, config: Config): SparkJobValidation = ???
}
```

- `runJob` contains the implementation of the Job. The SparkContext is managed by the JobServer and will be provided to the job through this method.
  This relieves the developer from the boiler-plate configuration management that comes with the creation of a Spark job and allows the Job Server to
manage and re-use contexts.
- `validate` allows for an initial validation of the context and any provided configuration. If the context and configuration are OK to run the job, returning `spark.jobserver.SparkJobValid` will let the job execute, otherwise returning `spark.jobserver.SparkJobInvalid(reason)` prevents the job from running and provides means to convey the reason of failure. In this case, the call immediately returns an `HTTP/1.1 400 Bad Request` status code.
`validate` helps you preventing running jobs that will eventually fail due to missing or wrong configuration and save both time and resources.

### NEW SparkJob API

Note: As of version 0.7.0, a new SparkJob API that is significantly better than the old SparkJob API will take over.  Existing jobs should continue to compile against the old `spark.jobserver.SparkJob` API, but this will be deprecated in the future.  Note that jobs before 0.7.0 will need to be recompiled, older jobs may not work with the current SJS example.  The new API looks like this:

```scala
object WordCountExampleNewApi extends NewSparkJob {
  type JobData = Seq[String]
  type JobOutput = collection.Map[String, Long]

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
    sc.parallelize(data).countByValue

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
    JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
```

It is much more type safe, separates context configuration, job ID, named objects, and other environment variables into a separate JobEnvironment input, and allows the validation method to return specific data for the runJob method.  See the [WordCountExample](job-server-tests/src/main/scala/spark/jobserver/WordCountExample.scala) and [LongPiJob](job-server-tests/src/main/scala/spark/jobserver/LongPiJob.scala) for examples.

Let's try running our sample job with an invalid configuration:

    curl -i -d "bad.input=abc" "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample"
    HTTP/1.1 400 Bad Request
    Server: spray-can/1.3.4
    Date: Thu, 14 Sep 2017 12:01:37 GMT
    Access-Control-Allow-Origin: *
    Content-Type: application/json; charset=UTF-8
    Content-Length: 738

    {
      "status": "VALIDATION FAILED",
      "result": {
        "message": "One(SparkJobInvalid(No input.string config param))",
        "errorClass": "java.lang.Throwable",
        "stack": "java.lang.Throwable: One(SparkJobInvalid(No input.string config param))\n\tat spark.jobserver.JobManagerActor$$anonfun$getJobFuture$4.apply(JobManagerActor.scala:327)\n\tat scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)\n\tat scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)\n\tat java.lang.Thread.run(Thread.java:748)\n"
      }
    }

### NEW SparkJob API with Spark v2.1

Deploying Spark JobServer with Spark v2.x cluster, you can create a SparkSession context which enables Spark-SQL and Hive support
```scala
curl -i -d "" 'http://localhost:8090/contexts/sql-context-1?num-cpu-cores=2&memory-per-node=512M&context-factory=spark.jobserver.context.SessionContextFactory'
```
Spark JobServer application shall extend from the SparkSessionJob to use the spark.jobserver.context.SessionContextFactory, here is an example
```scala
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalactic._
import spark.jobserver.SparkSessionJob
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}

import scala.util.Try

object WordCountExampleSparkSession extends SparkSessionJob {
  type JobData = Seq[String]
  type JobOutput = collection.Map[String, Long]

  override def runJob(sparkSession: SparkSession, runtime: JobEnvironment, data: JobData): JobOutput =
    sparkSession.sparkContext.parallelize(data).countByValue

  override def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
```

### Dependency jars

For Java/Scala applications you have a couple options to package and upload dependency jars.

* The easiest is to use something like [sbt-assembly](https://github.com/sbt/sbt-assembly) to produce a fat jar.  Be sure to mark the Spark and job-server dependencies as "provided" so it won't blow up the jar size.  This works well if the number of dependencies is not large.
* When the dependencies are sizeable and/or you don't want to load them with every different job, you can package the dependencies separately and use one of several options:
    - Use the `dependent-jar-uris` context configuration param. Then the jar gets loaded for every job.
    - The `dependent-jar-uris` can also be used in job configuration param when submitting a job. On an ad-hoc context this has the same effect as `dependent-jar-uris` context configuration param. On a persistent context the jars will be loaded for the current job and then for every job that will be executed on the persistent context.
        ````
        curl -d "" "localhost:8090/contexts/test-context?num-cpu-cores=4&memory-per-node=512m"
        OK⏎
        ````
        ````
        curl "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample&context=test-context&sync=true" -d '{
            dependent-jar-uris = ["file:///myjars/deps01.jar", "file:///myjars/deps02.jar"],
            input.string = "a b c a b see"
        }'
        ````
        The jars /myjars/deps01.jar & /myjars/deps02.jar (present only on the SJS node) will be loaded and made available for the Spark driver & executors.
        Please note that only only `file`, `local`, `ftp`, `http` protocols will work (URIs will be added to standard java class loader).
        Recent changes also allow to use names of the binaries, which were uploaded to Jobserver.
    - Use the `--package` option with Maven coordinates with `server_start.sh`.
    - Recent changes also allow you to use new parameters for the `POST /jobs` request:
      ````
      POST /jobs?cp=someURI,binName1,binName2&mainClass=some.main.Class
      ````
      `cp` accepts list of binary names (under which you uploaded binary to Jobserver) and URIs,
      `mainClass` is the main class of your application.
      Main advantage of this approach in comparison to using `dependent-jar-uris` is that you don't need to
      specify which jar is the main one and can just send all of needed jars in one list.

### Named Objects
#### Using Named RDDs
Initially, the job server only supported Named RDDs. For backwards compatibility and convenience, the following is still supported even though it is now possible to use the more generic Named Object support described in the next section.

Named RDDs are a way to easily share RDDs among jobs. Using this facility, computed RDDs can be cached with a given name and later on retrieved.
To use this feature, the SparkJob needs to mixin `NamedRddSupport`:
```scala
object SampleNamedRDDJob  extends SparkJob with NamedRddSupport {
    override def runJob(sc:SparkContext, jobConfig: Config): Any = ???
    override def validate(sc:SparkContext, config: Config): SparkJobValidation = ???
}
```

Then in the implementation of the job, RDDs can be stored with a given name:
```scala
this.namedRdds.update("french_dictionary", frenchDictionaryRDD)
```
Other job running in the same context can retrieve and use this RDD later on:
```scala
val rdd = this.namedRdds.get[(String, String)]("french_dictionary").get
```
(note the explicit type provided to get. This will allow to cast the retrieved RDD that otherwise is of type RDD[_])

For jobs that depends on a named RDDs it's a good practice to check for the existence of the NamedRDD in the `validate` method as explained earlier:
```scala
def validate(sc:SparkContext, config: Config): SparkJobValidation = {
  ...
  val rdd = this.namedRdds.get[(Long, scala.Seq[String])]("dictionary")
  if (rdd.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named RDD [dictionary]")
}
```
#### Using Named Objects
Named Objects are a way to easily share RDDs, DataFrames or other objects among jobs. Using this facility, computed objects can be cached with a given name and later on retrieved.
To use this feature, the SparkJob needs to mixin `NamedObjectSupport`. It is also necessary to define implicit persisters for each desired type of named objects. For convencience, we have provided implementations for RDD persistence and for DataFrame persistence (defined in `job-server-extras`):
```scala
object SampleNamedObjectJob  extends SparkJob with NamedObjectSupport {

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit val dataFramePersister = new DataFramePersister

    override def runJob(sc:SparkContext, jobConfig: Config): Any = ???
    override def validate(sc:SparkContext, config: Config): SparkJobValidation = ???
}
```

Then in the implementation of the job, RDDs can be stored with a given name:
```scala
this.namedObjects.update("rdd:french_dictionary", NamedRDD(frenchDictionaryRDD, forceComputation = false, storageLevel = StorageLevel.NONE))
```
DataFrames can be stored like so:
```scala
this.namedObjects.update("df:some df", NamedDataFrame(frenchDictionaryDF, forceComputation = false, storageLevel = StorageLevel.NONE))
```
It is advisable to use different name prefixes for different types of objects to avoid confusion.

Another job running in the same context can retrieve and use these objects later on:
```scala
val NamedRDD(frenchDictionaryRDD, _ ,_) = namedObjects.get[NamedRDD[(String, String)]]("rdd:french_dictionary").get

val NamedDataFrame(frenchDictionaryDF, _, _) = namedObjects.get[NamedDataFrame]("df:some df").get

```
(Note the explicit type provided to get. This will allow to cast the retrieved RDD/DataFrame object to the proper result type.)

For jobs that depends on a named objects it's a good practice to check for the existence of the NamedObject in the `validate` method as explained earlier:
```scala
def validate(sc:SparkContext, config: Config): SparkJobValidation = {
  ...
  val obj = this.namedObjects.get("dictionary")
  if (obj.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named object [dictionary]")
}
```

### HTTPS / SSL Configuration
#### Server authentication
To activate server authentication and ssl communication, set these flags in your application.conf file (Section 'spray.can.server'):
```
  ssl-encryption = on
  # absolute path to keystore file
  keystore = "/some/path/sjs.jks"
  keystorePW = "changeit"
```

You will need a keystore that contains the server certificate. The bare minimum is achieved with this command which creates a self-signed certificate:
```
 keytool -genkey -keyalg RSA -alias jobserver -keystore ~/sjs.jks -storepass changeit -validity 360 -keysize 2048
```
You may place the keystore anywhere.
Here is an example of a simple curl command that utilizes ssl:
```
curl -k https://localhost:8090/contexts
```
The ```-k``` flag tells curl to "Allow connections to SSL sites without certs". Export your server certificate and import it into the client's truststore to fully utilize ssl security.

#### Client authentication
Client authentication can be enabled by simply pointing Job Server to a valid Trust Store. 
As for server authentication, this is done by setting appropriate values in the application.conf.
The minimum set of parameters to enable client authentication consists of:
```
  # truststore = "/some/path/server-truststore.jks"
  # truststorePW = "changeit"
```
Note, client authentication implies server authentication, therefore client authentication will only be enabled once server authentication is activated.

### Basic authentication
Basic authentication (username and password) in Job Server relies on the [Apache Shiro](http://shiro.apache.org/index.html) framework. 
Basic authentication is activated by setting this flag (Section 'shiro'):
```
authentication = on
# absolute path to shiro config file, including file name
config.path = "/some/path/shiro.ini"
```
Shiro-specific configuration options should be placed into a file named 'shiro.ini' in the directory as specified by the config option 'config.path'.
Here is an example that configures LDAP with user group verification:
```
# use this for basic ldap authorization, without group checking
# activeDirectoryRealm = org.apache.shiro.realm.ldap.JndiLdapRealm
# use this for checking group membership of users based on the 'member' attribute of the groups:
activeDirectoryRealm = spark.jobserver.auth.LdapGroupRealm
# search base for ldap groups (only relevant for LdapGroupRealm):
activeDirectoryRealm.contextFactory.environment[ldap.searchBase] = dc=xxx,dc=org
# allowed groups (only relevant for LdapGroupRealm):
activeDirectoryRealm.contextFactory.environment[ldap.allowedGroups] = "cn=group1,ou=groups", "cn=group2,ou=groups"
activeDirectoryRealm.contextFactory.environment[java.naming.security.credentials] = password
activeDirectoryRealm.contextFactory.url = ldap://localhost:389
activeDirectoryRealm.userDnTemplate = cn={0},ou=people,dc=xxx,dc=org

cacheManager = org.apache.shiro.cache.MemoryConstrainedCacheManager

securityManager.cacheManager = $cacheManager
```

Make sure to edit the url, credentials, userDnTemplate, ldap.allowedGroups and ldap.searchBase settings in accordance with your local setup.

Here is an example of a simple curl command that authenticates a user and uses ssl (you may want to use -H to hide the
credentials, this is just a simple example to get you started):
```
curl -k --basic --user 'user:pw' https://localhost:8090/contexts
```

## Deployment

See also running on [cluster](doc/cluster.md), [YARN client](doc/yarn.md), on [EMR](doc/EMR.md) and running on [Mesos](doc/mesos.md).

### Manual steps

1. Copy `config/local.sh.template` to `<environment>.sh` and edit as appropriate.  NOTE: be sure to set SPARK_VERSION if you need to compile against a different version.
2. Copy `config/shiro.ini.template` to `shiro.ini` and edit as appropriate. NOTE: only required when `authentication = on`
3. Copy `config/local.conf.template` to `<environment>.conf` and edit as appropriate.
4. `bin/server_deploy.sh <environment>` -- this packages the job server along with config files and pushes
   it to the remotes you have configured in `<environment>.sh`
5. On the remote server, start it in the deployed directory with `server_start.sh` and stop it with `server_stop.sh`

The `server_start.sh` script uses `spark-submit` under the hood and may be passed any of the standard extra arguments from `spark-submit`.

NOTE: Under the hood, the deploy scripts generate an assembly jar from the `job-server-extras` project.  Generating assemblies from other projects may not include all the necessary components for job execution.

### Context per JVM

Each context can be a separate process launched using SparkLauncher, if `context-per-jvm` is set to true.
This can be especially desirable when you want to run many contexts at once, or for certain types of contexts such as StreamingContexts which really need their own processes.

Also, the extra processes talk to the master HTTP process via random ports using the Akka Cluster gossip protocol.  If for some reason the separate processes causes issues, set `spark.jobserver.context-per-jvm` to `false`, which will cause the job server to use a single JVM for all contexts.

Among the known issues:
- Launched contexts do not shut down by themselves.  You need to manually kill each separate process, or do `-X DELETE /contexts/<context-name>`

Log files are separated out for each context (assuming `context-per-jvm` is `true`) in their own subdirs under the `LOG_DIR` configured in `settings.sh` in the deployed directory.

Note: to test out the deploy to a local staging dir, or package the job server for Mesos,
use `bin/server_package.sh <environment>`.

### Configuring Spark Jobserver backend

Spark Jobserver offers a variety of options for backend storage such as:
- H2/PostreSQL or other SQL Databases
- Cassandra
- Combination of SQL DB or Zookeeper with HDFS

#### Configuring Spark Jobserver H2 Database backend
By default, H2 database is used for storing Spark Jobserver related meta data.
This can be overridden if you prefer to use PostgreSQL or MySQL.
It is also important that any dependent jars are to be added to Job Server class path.

To use the embedded H2 db as a backend, add the following configuration to local.conf.

    spark {
      jobserver {
        ...
        sqldao {
          # Slick database driver, full classpath
          slick-driver = slick.driver.H2Driver

          # JDBC driver, full classpath
          jdbc-driver = org.h2.Driver

          # Directory where binaries are cached and default H2 driver stores its data
          rootdir = "/var/spark-jobserver/sqldao/data"

          jdbc {
            url = "jdbc:h2:file:/var/spark-jobserver/sqldao/data/h2-db"
            user = "secret"
            password = "secret"
          }

          dbcp {
            maxactive = 20
            maxidle = 10
            initialsize = 10
          }
        }
      }
    }
    # also add the following line at the root level.
    flyway.locations="db/h2/migration"

If you are using `context-per-jvm = true`, be sure to add [AUTO_MIXED_MODE](http://h2database.com/html/features.html#auto_mixed_mode) to your
H2 JDBC URL; this allows multiple processes to share the same H2 database using a lock file.

In yarn-client mode, use H2 in server mode as described below instead of embedded mode.
- Download the full H2 jar from http://www.h2database.com/html/download.html and follow docs.
- Note that the version of H2 should match the H2 client version bundled with spark-jobserver, currently 1.3.176.


A sample JDBC configuration is below:
```
jdbc {
        url = "jdbc:h2:tcp://localhost//ROOT/PARENT/DIRECTORIES/spark_jobserver"
        user = "secret"
        password = "secret"
      }

```
Note: /ROOT/PARENT/DIRECTORIES/spark_jobserver is the absolute path to a directory to which H2 has write access.


Example command line to launch H2 Server:
```
java -cp h2-1.3.176.jar org.h2.tools.Server -tcp
```
Use -? on command line to see other options.

#### Configuring Spark Jobserver PostgreSQL Database backend
Ensure that you have spark_jobserver database created with necessary rights
granted to user.

    # create database user jobserver and database spark_jobserver:
    $ createuser --username=<superuser> -RDIElPS jobserver
    $ createdb -Ojobserver -Eutf8 spark_jobserver
    CTRL-D -> logout from psql

    # logon as superuser and enable the large object extension:
    $ psql -U <superuser> spark_jobserver
    spark_jobserver=# CREATE EXTENSION lo;
    CTRL-D -> logout from psql

    # you can connect to the database using the psql command line client:
    $ psql -U jobserver spark_jobserver

To use PostgreSQL as backend add the following configuration to local.conf.

    spark {
      jobserver {
        ...
        sqldao {
          # Slick database driver, full classpath
          slick-driver = slick.driver.PostgresDriver

          # JDBC driver, full classpath
          jdbc-driver = org.postgresql.Driver

          # Directory where default H2 driver stores its data. Only needed for H2.
          rootdir = "/var/spark-jobserver/sqldao/data"

          jdbc {
            url = "jdbc:postgresql://db_host/spark_jobserver"
            user = "jobserver"
            password = "secret"
          }

          dbcp {
            maxactive = 20
            maxidle = 10
            initialsize = 10
          }
        }
      }
    }
    # also add the following line at the root level.
    flyway.locations="db/postgresql/migration"

#### Configuring Spark Jobserver MySQL Database backend
Ensure that you have spark_jobserver database created with necessary rights
granted to user.

    # secure your mysql installation and define password for mysql root user
    $ mysql_secure_installation

    # logon as database root
    $ mysql -u root -p

    # create a database user and a database for spark jobserver:
    mysql> CREATE USER 'jobserver'@'localhost' IDENTIFIED BY 'secret';
    mysql> CREATE DATABASE spark_jobserver;
    mysql> GRANT ALL ON spark_jobserver.* TO 'jobserver'@'localhost';
    mysql> FLUSH PRIVILEGES;
    CTRL-D -> logout from mysql

    # you can connect to the database using the mysql command line client:
    $ mysql -u jobserver -p

To use MySQL as backend add the following configuration to local.conf.

    spark {
      jobserver {
        ...
        sqldao {
          # Slick database driver, full classpath
          slick-driver = slick.driver.MySQLDriver

          # JDBC driver, full classpath
          jdbc-driver = com.mysql.jdbc.Driver

          jdbc {
            url = "jdbc:mysql://db_host/spark_jobserver"
            user = "jobserver"
            password = "secret"
          }

          dbcp {
            maxactive = 20
            maxidle = 10
            initialsize = 10
          }
        }
      }
    }
    # also add the following line at the root level.
    flyway.locations="db/mysql/migration"

#### Configuring Spark Jobserver Zookeeper + HDFS Database backend

To use Zookeeper (for metadata) and HDFS (for binaries) as backend add the following
configuration to local.conf.

```
    combineddao {
      rootdir = "/tmp/combineddao"
      binarydao {
        class = spark.jobserver.io.HdfsBinaryDAO
        dir = "hdfs:///spark-jobserver/binaries"
      }
      metadatadao {
        class = spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
      }
    }

    zookeeperdao {
      dir = "jobserver/db"
      connection-string = "localhost:2181"
    }
```

More information on setting up different backends for binaries and jobserver meta data: [setting up dao](doc/dao-setup.md).

#### Configuring Spark Jobserver Cassandra backend

To use Cassandra db as a backend, setup Cassandra and add the following configuration to local.conf:

```
    cassandra {
      consistency = "ONE"
      hosts = ["localhost:9042"]
      user = ""
      password = ""
      chunk-size-in-kb = 1024
    }

    cassandradao {
      # Directory where Jobserver will cache files before starting the job
      rootdir = /tmp/spark-jobserver/cassandradao/data
    }
```


### HA Deployment (beta)

It is possible to run multiple Spark Jobservers in a highly available setup. For a documentation of a Jobserver HA setup, refer to the [Jobserver HA documentation](doc/HA.md).

### Chef

There is also a [Chef cookbook](https://github.com/spark-jobserver/chef-spark-jobserver) which can be used to deploy Spark Jobserver.

## Architecture

The job server is intended to be run as one or more independent processes, separate from the Spark cluster
(though it very well may be collocated with say the Master).

At first glance, it seems many of these functions (eg job management) could be integrated into the Spark standalone master.  While this is true, we believe there are many significant reasons to keep it separate:

- We want the job server to work for Mesos and YARN as well
- Spark and Mesos masters are organized around "applications" or contexts, but the job server supports running many discrete "jobs" inside a single context
- We want it to support Shark functionality in the future
- Loose coupling allows for flexible HA arrangements (multiple job servers targeting same standalone master, or possibly multiple Spark clusters per job server)

Flow diagrams are checked in in the doc/ subdirectory.  .diagram files are for websequencediagrams.com... check them out, they really will help you understand the flow of messages between actors.

## API

### Binaries

    GET /binaries               - lists all current binaries
    GET /binaries /<appName>    - gets info about the last binary uploaded under this name (app-name, binary-type, upload-time)
    POST /binaries/<appName>    - upload a new binary file
    DELETE /binaries/<appName>  - delete defined binary

When POSTing new binaries, the content-type header must be set to one of the types supported by the subclasses of the `BinaryType` trait. e.g. "application/java-archive" or application/python-archive". If you are using curl command, then you must pass "-H 'Content-Type: application/python-archive'" or "-H 'Content-Type: application/java-archive'".

### Contexts

    GET /contexts               - lists all current contexts
    GET /contexts/<name>        - gets info about a context, such as the spark UI url
    POST /contexts/<name>       - creates a new context
    DELETE /contexts/<name>     - stops a context and all jobs running in it. Additionally, you can pass ?force=true to stop a context forcefully. This is equivalent to killing the application from SparkUI (works for spark standalone only).
    PUT /contexts?reset=reboot  - shuts down all contexts and re-loads only the contexts from config. Use ?sync=false to execute asynchronously.

Spark context configuration params can follow `POST /contexts/<name>` as query params. See section below for more details.

### Jobs

Jobs submitted to the job server must implement a `SparkJob` trait.  It has a main `runJob` method which is
passed a SparkContext and a typesafe Config object.  Results returned by the method are made available through
the REST API.

    GET /jobs                - Lists the last N jobs
    POST /jobs               - Starts a new job, use ?sync=true to wait for results
    GET /jobs/<jobId>        - Gets the result or status of a specific job
    DELETE /jobs/<jobId>     - Kills the specified job
    GET /jobs/<jobId>/config - Gets the job configuration

For additional information on `POST /jobs` check out [submitting jobs documentation](doc/submitting-jobs.md).

For details on the Typesafe config format used for input (JSON also works), see the [Typesafe Config docs](https://github.com/typesafehub/config).

### Data

It is sometime necessary to programmatically upload files to the server. Use these paths to manage such files:

    GET /data                - Lists previously uploaded files that were not yet deleted
    POST /data/<prefix>      - Uploads a new file, the full path of the file on the server is returned, the
                               prefix is the prefix of the actual filename used on the server (a timestamp is
                               added to ensure uniqueness)
    DELETE /data/<filename>  - Deletes the specified file (only if under control of the JobServer)
    PUT /data?reset=reboot   - Deletes all uploaded files. Use ?sync=false to execute asynchronously.

These files are uploaded to the server and are stored in a local temporary
directory where the JobServer runs. The POST command returns the full
pathname and filename of the uploaded file so that later jobs can work with this
just the same as with any other server-local file. A job could therefore add this file to HDFS or distribute
it to worker nodes via the SparkContext.addFile command.
For files that are larger than a few hundred MB, it is recommended to manually upload these files to the server or
to directly add them to your HDFS.

#### Data API Example

    $ curl -d "Test data file api" http://localhost:8090/data/test_data_file_upload.txt
    {
      "result": {
        "filename": "/tmp/spark-jobserver/upload/test_data_file_upload.txt-2016-07-04T09_09_57.928+05_30.dat"
      }
    }

    $ curl http://localhost:8090/data
    ["/tmp/spark-jobserver/upload/test_data_file_upload.txt-2016-07-04T09_09_57.928+05_30.dat"]

    $ curl -X DELETE http://localhost:8090/data/%2Ftmp%2Fspark-jobserver%2Fupload%2Ftest_data_file_upload.txt-2016-07-04T09_09_57.928%2B05_30.dat
    OK

    $ curl http://localhost:8090/data
    []

Note: Both POST and DELETE requests takes URI encoded file names.

### Context configuration

A number of context-specific settings can be controlled when creating a context (POST /contexts) or running an
ad-hoc job (which creates a context on the spot).  For example, add urls of dependent jars for a context.

    POST '/contexts/my-new-context?dependent-jar-uris=file:///some/path/of/my-foo-lib.jar'

NOTE: Only the latest `dependent-jar-uris` (btw it’s jar-uris, not jars-uri) takes effect.  You can specify multiple URIs by comma-separating them.  So like this:

    &dependent-jar-uris=file:///path/a.jar,file:///path/b.jar

When creating a context via POST /contexts, the query params are used to override the default configuration in
spark.context-settings.  For example,

    POST /contexts/my-new-context?num-cpu-cores=10

would override the default spark.context-settings.num-cpu-cores setting.

When starting a job, and the `context=` query param is not specified, then an ad-hoc context is created.  Any
settings specified in spark.context-settings will override the defaults in the job server config when it is
started up.

Any spark configuration param can be overridden either in POST /contexts query params, or through `spark
.context-settings` job configuration.  In addition, `num-cpu-cores` maps to `spark.cores.max`, and `mem-per-
node` maps to `spark.executor.memory`.  Therefore the following are all equivalent:

    POST /contexts/my-new-context?num-cpu-cores=10

    POST /contexts/my-new-context?spark.cores.max=10

or in the job config when using POST /jobs,

    spark.context-settings {
        spark.cores.max = 10
    }

User impersonation for an already Kerberos authenticated user is supported via `spark.proxy.user` query param:

  POST /contexts/my-new-context?spark.proxy.user=<user-to-impersonate>

However, whenever the flag `shiro.use-as-proxy-user` is set to `on` (and authentication is `on`) then this parameter
is ignored and the name of the authenticated user is *always* used as the value of the `spark.proxy.user`
parameter when creating contexts.

To pass settings directly to the sparkConf that do not use the "spark." prefix "as-is", use the "passthrough" section.

    spark.context-settings {
        spark.cores.max = 10
        passthrough {
          some.custom.hadoop.config = "192.168.1.1"
        }
    }

To add to the underlying Hadoop configuration in a Spark context, add the hadoop section to the context settings

    spark.context-settings {
        hadoop {
            mapreduce.framework.name = "Foo"
        }
    }

`stop-context-on-job-error=true` can be passed to context if you want the context to stop immediately after first error is reported by a job. The default value is false.

For the exact context configuration parameters, see JobManagerActor docs as well as application.conf.

### Other configuration settings

For all of the Spark Job Server configuration settings, see `job-server/src/main/resources/application.conf`.

### Job Result Serialization

The result returned by the `SparkJob` `runJob` method is serialized by the job server into JSON for routes
that return the result (GET /jobs with sync=true, GET /jobs/<jobId>).  Currently the following types can be
serialized properly:

- String, Int, Long, Double, Float, Boolean
- Scala Map's with string key values (non-string keys may be converted to strings)
- Scala Seq's
- Array's
- Anything that implements Product (Option, case classes) -- they will be serialized as lists
- Subclasses of java.util.List
- Subclasses of java.util.Map with string key values (non-string keys may be converted to strings)
- Maps, Seqs, Java Maps and Java Lists may contain nested values of any of the above
- If a job result is of scala's Stream[Byte] type it will be serialised directly as a chunk encoded stream.
  This is useful if your job result payload is large and may cause a timeout serialising as objects. Beware, this
  will not currently work as desired with context-per-jvm=true configuration, since it would require serialising
  Stream[\_] blob between processes. For now use Stream[\_] job results in context-per-jvm=false configuration, pending
  potential future enhancements to support this in context-per-jvm=true mode.

If we encounter a data type that is not supported, then the entire result will be serialized to a string.

### HTTP Override

Spark Job Server offers HTTP override functionality.
Often reverse proxies and firewall implement access limitations to, for example, DELETE and PUT requests.
HTTP override allows overcoming these limitations by wrapping, for example, a DELETE request into a POST request.

Requesting the destruction of a context can be accomplished through HTTP override using the following syntax:

    $ curl -X POST "localhost:8090/contexts/test_context?_method=DELETE"

Here, a DELETE request is passed to Spark Job Server "through" a POST request.


## Clients

Spark Jobserver project has a
[python binding](https://github.com/spark-jobserver/python-sjsclient) package.
This can be used to quickly develop python applications that can interact with
Spark Jobserver programmatically.

## Contribution and Development
Contributions via Github Pull Request are welcome. Please start by taking a look at the [contribution guidelines](doc/contribution-guidelines.md) and check the TODO for some contribution ideas.

- If you need to build with a specific scala version use ++x.xx.x followed by the regular command,
for instance: `sbt ++2.11.6 job-server/compile`
- From the "master" project, please run "test" to ensure nothing is broken.
   - You may need to set `SPARK_LOCAL_IP` to `localhost` to ensure Akka port can bind successfully
   - Note for Windows users: very few tests fail on Windows. Thus, run `testOnly -- -l WindowsIgnore` from SBT shell to ignore them.
- Logging for tests goes to "job-server-test.log". To see test logging in console also, add the following to your log4j.properties (`job-server/src/test/resources/log4j.properties`)
```$xslt
log4j.rootLogger=INFO, LOGFILE, console

log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=[%d] %-5p %.26c [%X{testName}] [%X{akkaSource}] - %m%n
```
- Run `sbt clean coverage test` to check the code coverage and improve it. You can generate reports by running
`sbt coverageReport` or `sbt coverageAggregate` for the full overview.
  - Windows users: run `; coverage ; testOnly -- -l WindowsIgnore ; coverageReport` from SBT shell.
- Please run scalastyle to ensure your code changes don't break the style guide.
- Do "reStart" from SBT for quick restarts of the job server process
- Please update the g8 template if you change the SparkJob API

Profiling software generously provided by ![](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with its full-featured [Java Profiler](https://www.yourkit.com/java/profiler/index.jsp).

### Publishing packages

In the root project, do `release cross`.

To announce the release on [ls.implicit.ly](http://ls.implicit.ly/), use
[Herald](https://github.com/n8han/herald#install) after adding release notes in
the `notes/` dir.  Also regenerate the catalog with `lsWriteVersion` SBT task
and `lsync`, in project job-server.

## Contact

For user/dev questions, we are using google group for discussions:
<https://groups.google.com/forum/#!forum/spark-jobserver>

Please report bugs/problems to:
<https://github.com/spark-jobserver/spark-jobserver/issues>

## License
Apache 2.0, see LICENSE.md

## TODO

- More debugging for classpath issues
- Add Swagger support.  See the spray-swagger project.
- Implement an interactive SQL window.  See: [spark-admin](https://github.com/adatao/spark-admin)

- Stream the current job progress via a Listener
- Add routes to return stage info for a job.  Persist it via DAO so that we can always retrieve stage / performance info
  even for historical jobs.  This would be pretty kickass.
