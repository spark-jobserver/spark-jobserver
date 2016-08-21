## Custom Contexts and Context-specific Jobs

With Spark Jobserver 0.5.0, jobs no longer have to share just a plain
`SparkContext`, but can share other types of contexts as well, such as a
`SQLContext` or `HiveContext`.  This allows Spark jobs to share the state of 
other contexts, such as SQL temporary tables.  An example can be found in the 
`SQLLoaderJob` class, which creates a temporary table, and the `SQLTestJob` job, 
which runs a SQL query against the loaded table.  This feature can also be 
used with other contexts than the ones supplied by Spark itself, such as the 
CassandraContext from Datastax's Cassandra Spark Connector.

## Example

NOTE: To run these examples, you can either use `job-server-extras/reStart` from SBT, or you
can run `bin/server_package.sh`, edit `/tmp/job-server/settings.sh` to point at your local Spark repo (with binaries
built), then run `/tmp/job-server/server_start.sh`.

To run jobs for a specific type of context, first you need to start a context with the `context-factory` param:

    curl -d "" '127.0.0.1:8090/contexts/sql-context?context-factory=spark.jobserver.context.SQLContextFactory'
    OK⏎

Similarly, to use a HiveContext for jobs pass `context-factory=spark.jobserver.context.HiveContextFactory`, but be sure to run the `HiveTestJob` instead below.

Package up the job-server-extras example jar:

    sbt 'job-server-extras/package'

Load it to job server:

    curl --data-binary @job-server-extras/job-server-extras/target/scala-2.10/job-server-extras_2.10-0.6.2-SNAPSHOT-tests.jar  127.0.0.1:8090/jars/sql

Now you should be able to run jobs in that context.  Note that SQL has to be quoted carefully when you are using curl.

    curl -d "" '127.0.0.1:8090/jobs?appName=sql&classPath=spark.jobserver.SqlLoaderJob&context=sql-context&sync=true'

    curl -d "sql = \"select * from addresses limit 10\"" '127.0.0.1:8090/jobs?appName=sql&classPath=spark.jobserver.SqlTestJob&context=sql-context&sync=true'
    
NOTE: you will get an error if you run the wrong type of job, such as a regular SparkJob in a `SQLContext`.

## Initializing a Hive/SQLContext Automatically

You can skip the steps of context creation and jar upload with the latest job server using some config options.  
Add the following to your job server config (the deprecated `job-jar-paths` will also work):

```apache
spark {
  jobserver {
    # Automatically load a set of jars at startup time.  Key is the appName, value is the path/URL.
    job-binary-paths {    # NOTE: you may need an absolute path below
      sql = job-server-extras/target/scala-2.10/job-server-extras_2.10-0.6.2-SNAPSHOT-tests.jar
    }
  }
  
  contexts {
    sql-context {
      num-cpu-cores = 1           # Number of cores to allocate.  Required.
      memory-per-node = 512m         # Executor memory per node, -Xmx style eg 512m, 1G, etc.
      context-factory = spark.jobserver.context.HiveContextFactory
    }
  }  
}
```

Now, when you start up the job server, you will see a context `sql-context` and a jar app `sql` pre-loaded, and you can execute your SQL queries immediately (assuming you have tables stored in your Hive Metastore).

NOTE: The above also works on DSE 4.8, which packages Job Server 0.5.2, but you need to edit the default configuration in `resources/spark/spark-jobserver/dse.conf`.

## Extending Job Server for Custom Contexts

This can be done easily by extending the `SparkContextFactory` trait, like `SQLContextFactory` does.  Then, extend the `api.SparkJobBase` trait in a job with a type matching your factory.

NOTE: If you have defined custom `ContextFactory`s from before 0.7.0, you will need to modify them as the `isValidJob` signature has changed.

## Jars

If you wish to use the `SQLContext` or `HiveContext`, be sure to pull down the job-server-extras package.

## StreamingContext

`job-server-extras` provides a context to run Spark Streaming jobs. There are a couple of configurations you can change in job-server's .conf file:

* `streaming.batch_interval`: the streaming batch in millis
* `streaming.stopGracefully`: if true, stops gracefully by waiting for the processing of all received data to be completed 
* `streaming.stopSparkContext`: if true, stops the SparkContext with the StreamingContext. The underlying SparkContext will be stopped regardless of whether the StreamingContext has been started.

### Running Multiple HiveContexts (Thanks cgeorge-rms)

This isn't an issue, but wanted to give everyone heads up if someone is searching on this problem.
When running `context-per-jvm=true` and running multiple HiveContexts without using a shared mysql database you will get an exception about derby locking.
I found that if you put a hive-site.xml in spark/conf directory containing:


    javax.jdo.option.ConnectionURL
    jdbc:derby:memory:myDB;create=true
    JDBC connect string for a JDBC metastore


    javax.jdo.option.ConnectionDriverName
    org.apache.derby.jdbc.EmbeddedDriver
    Driver class name for a JDBC metastore

It will then create an in memory derby instance for the hive metastore (this is assuming you don't need persistent data stored in actual hive metastore) 
We are doing this because we want context isolation and are running HiveServer2 from a shared context for jdbc access which works really well for us.
