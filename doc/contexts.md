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

## Extending Job Server for Custom Contexts

This can be done easily by extending the `SparkContextFactory` trait, like `SQLContextFactory` does.  Then, extend the `SparkJobBase` trait in a job with a type matching your factory.

## Jars

If you wish to use the `SQLContext` or `HiveContext`, be sure to pull down the job-server-extras package.

## StreamingContext

`job-server-extras` provides a context to run Spark Streaming jobs. There are a couple of configurations you can change in job-server's .conf file:

* `streaming.batch_interval`: the streaming batch in millis
* `streaming.stopGracefully`: if true, stops gracefully by waiting for the processing of all received data to be completed 
* `streaming.stopSparkContext`: if true, stops the SparkContext with the StreamingContext. The underlying SparkContext will be stopped regardless of whether the StreamingContext has been started.

