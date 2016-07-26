# Troubleshooting

## Tests don't pass or have timeouts

If you used `reStart` to start a local job server, be sure it's stopped using `reStop` first.

Also, try adding `-Dakka.test.timefactor=X` to `SBT_OPTS` before launching sbt, where X is a number greater than 1.  This scales out the Akka TestKit timeouts by a factor X.

## Requests are timing out

or you get `akka.pattern.AskTimeoutException`.

send timeout param along with your request (in secs). eg below.

```
http://devsparkcluster.cloudapp.net/jobs?appName=job-server-tests&classPath=spark.jobserver.WordCountExample&sync=true&timeout=20
```

You may need to adjust Spray's default request timeout and idle timeout, which are by default 40 secs and 60 secs.  To do this, modify the configuration file in your deployed job server, adding a section like the following:

```
spray.can.server {
  idle-timeout = 210 s
  request-timeout = 200 s
}
```

Then simply restart the job server.

Note that the idle-timeout must be higher than request-timeout, or Spray and the job server won't start.

## Timeout getting large job results

If your job returns a large job result, it may exceed Akka's maximum network message frame size, in which case the result is dropped and you may get a network timeout.  Change the following configuration, which defaults to 10 MiB:

    akka.remote.netty.tcp.maximum-frame-size = 100 MiB

## AskTimeout when starting job server or contexts

If you are loading large jars or dependent jars, either at startup or when creating a large context, the database such as H2 may take a really long time to write those bytes to disk.  You need to adjust the context timeout setting:

    spark.jobserver.context-creation-timeout

Set it to 60 seconds or longer, especially if your jars are in the many MBs.

NOTE: if you are running SJS in Docker, esp on AWS, you might need to enable host-only networking.

## Job server won't start / cannot bind to 0.0.0.0:8090

Check that another process isn't already using that port.  If it is, you may want to start it on another port:

    reStart --- -Dspark.jobserver.port=2020

## Job Server Doesn't Connect to Spark Cluster

Finally, I got the problem solved. There are two problems in my configuration:

1. the version of spark cluster is 1.1 but the spark version in job server machine is 1.0.2
after upgrading spark to 1.1 in job server machine, jobs can be submitted to spark cluster (can show in spark UI) but cannot be executed.
2. the spark machines need to know the host name of job server machine
after this fixed, I can run jobs submitted from a remote job server successfully.

(Thanks to @pcliu)

## Exception in thread "main" java.lang.NoSuchMethodError: akka.actor.ActorRefFactory.dispatcher()Lscala/concurrent/ExecutionContextExecutor;

If you are running CDH 5.3 or older, you may have an incompatible version of Akka bundled together.  :(  Fortunately, one of our users has put together a [branch that works](https://github.com/bjoernlohrmann/spark-jobserver/tree/cdh-5.3) ... try that out!

(Older instructions) Try modifying the version of Akka included with spark-jobserver to match the one in CDH (2.2.4, I think), or upgrade to CDH 5.4.   If you are on CDH 5.4, check that `sparkVersion` in `Dependencies.scala` matches CDH.  Or see [isse #154](https://github.com/spark-jobserver/spark-jobserver/issues/154).

## I am running CDH 5.3 and Job Server doesn't work

See above.

## I want to run job-server on Windows

1. Create directory `C:\Hadoop\bin`
2. Download `http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe` and place it in `C:\Hadoop\bin`
3. Set environment variable HADOOP_HOME (either in a .bat script or within OS properties)  `HADOOP_HOME=C:\Hadoop`
4. Start spark-job-server in a shell that has the HADOOP_HOME environment set.
5. Submit the WordCountExample Job.

(Thanks to Javier Delgadillo)

## Akka Deadletters / Workers disconnect from Job Server

Most likely a networking issue. Try using IP addresses instead of DNS.  (happens in AWS)

## java.lang.ClassNotFoundException when staring spark-jobserver from sbt

Symptom: 

You start from SBT using `reStart`, and when try to create a HiveContext or SQLContext, e.g. using context-factory=spark.jobserver.context.HiveContextFactory and get an error like this 
    {"status": "CONTEXT INIT ERROR",
      "result": {
        "message": "spark.jobserver.context.HiveContextFactory",
        "errorClass": "java.lang.ClassNotFoundException",
        ...
      }
    ...
    
Solution:

Before typing `reStart` in sbt, type `project job-server-extras` and only then start it using `reStart` 

## Accessing a config file in my job jar

```scala
ConfigFactory.parseReader(paramReader = new InputStreamReader(getClass().getResourceAsStream(s"/$myPassedConfigPath"))
```
