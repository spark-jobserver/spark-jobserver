<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Supervise Mode](#supervise-mode)
  - [Description of implementation within SJS](#description-of-implementation-within-sjs)
  - [What Happens with Contexts/Jobs in Supervise Mode?](#what-happens-with-contextsjobs-in-supervise-mode)
  - [Configurations required for supervise mode](#configurations-required-for-supervise-mode)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## [Supervise Mode](https://spark.apache.org/docs/latest/spark-standalone.html)
Note: This is an experimental feature

The supervise mode allows restarting a driver program on a different worker VM in a Spark cluster in case it died unexpectedly with a non-zero exit code.

Note that restarting an application is the easy part, but you need to be aware of potential side effects of re-running an application! For example, let's assume an application updated the balance of a bank account by $100. If you implement this update as a simply increment of the current balance, then re-running the application might increasing the balance by another $100, resulting in $200 in the account.

As a best practice it is strongly recommend to make the whole application idempotent. Idempotence means that re-running the application must not change state that has already been committed. This means, for instance, that you need a way to determine if a certain operation has already been performed, or not.

### Description of implementation within SJS
A global property `spark.driver.supervise` is introduced in SJS config file (with a default value of false).
The value can be set to `true` or `false`.

- True = Enable supervise mode by default for all the new contexts.
- False = Disable supervise mode for all contexts but user can decide if the context needs to be supervised or not.

For each context, a property `launcher.spark.driver.supervise` is provided, user can set individually for each context, if it should use this mode or not.

Here are all the possible cases:

| Default mode  |  Context mode  |    Action    |
| ------------- | -------------- | ------------ |
| true          |   true         |    enabled   |
| true          |   false        |    disabled  |
| false         |   true         |    enabled   |
| false         |   false        |    disabled  |
| true          |   Not defined  |    enabled   |
| false         |   Not defined  |    disabled  |



Curently, this mode is tested with streaming/batch jobs.
A few things to note
- The criteria for a restart:
    * Job is either in RUNNING state, or
    * Job is in ERROR state with Context Terminated
        exception as error message.
    * The job is async
- The jobs for restart are selected based on unique context Id. So, we won't get jobs from any other context.
- On master we detect a supervise scenario based on contextInitInfos hashmap. If the hashmap has an entry, it is a normal initialize of context but if it doesn't then it is a restart scenario. This approach is safe because contextInitInfos's key is the slaves job manager actor name, this name is a combination of jobManager-<uuid>. If the context was initialized properly then this entry will be removed for sure. If the initialization failed, then contextInitInfos might have the entry but it is useless because context was never initialized and restart is not possible.
- If an existing job is restarted successfully, we change the error status back to running and also purge the endtime

<b>Note: If supervise mode is active, JVMs will only be restarted if the JVM was killed with a non-zero exit code.</b>

Some behaviors of cluster + supervise mode:
- If we kill the application from MasterUI -> kills the app + driver and no restart
- If we kill the driver from Master UI -> kills app + driver and no restart
- If we kill the context from SJS -> Kills the driver + app and no restart
- If we kill the job from SJS -> Keep the driver + app running and no restart of the job
- If we kill the Driver Wrapper JVM using `kill -9 <pid>` -> Driver comes up with different PID but same driver ID,
    application ID changes, restart happens.
- If we kill the Executor JVM using `kill -9 <pid>` -> Drivers stays, app also stays, the executor JVM is
     relaunched with different pid, not related to Restart, normal behavior of Spark
- If we have a job which fails on the Executor and the executor restarts -> Driver stays, app also stays, executor JVM is relaunched.
    We can delete the job/context through Master UI and SJS.
- If we have a job which fails on the Driver -> Driver restarts (only if supervise mode is enabled), we can stop
     it through SJS, we can always stop it from UI.

Note: If due to some reason, some jobs are scheduled in Spark and you stop the context from SJS, it can become a zombie. SJS DELETE request is passed to context JVM but Spark context fails to stop. Due to this, delete request times out and context can become a zombie (i.e. SJS has no idea about context anymore but context is still running in Spark)

### What Happens with Contexts/Jobs in Supervise Mode?
* Streaming Contexts:
    If a streaming context is restarted, Spark Job Server will try to restart the streaming job in the context (if any). If the job restart fails, it will also kill the driver program.
* Batch Contexts:
    If multiple batch jobs are running in a context that is being restarted, then all the jobs will be restarted. If all the batch jobs running in a context fail to restart, then the context will be killed. However, if at least one job restarts successfully, then the context is kept running. Already finished jobs of course will not be restarted.
* If a context fails during restart, all jobs running inside this context will be marked as failed.

### Configurations required for supervise mode
- Cluster mode should be enabled, please refer to the [cluster.md](https://github.com/spark-jobserver/spark-jobserver/blob/master/doc/cluster.md), [mesos.md](https://github.com/spark-jobserver/spark-jobserver/blob/master/doc/mesos.md), [yarn.md](https://github.com/spark-jobserver/spark-jobserver/blob/master/doc/yarn.md)
- set `spark.driver.supervise` to `true` or if by default is disabled then for each context, you should enable it using `launcher.spark.driver.supervise` e.g. `curl -X POST localhost:8090/contexts/abc?launcher.spark.driver.supervise=true`
- `spark.jobserver.ignore-akka-hostname` should be configured according to your environment. This property is tricky and depends on your network configuration. Since in cluster mode, all the drivers run on Workers. Each driver is running Akka inside which requires an IP address and it should not be 127.0.0.1. So, if your network configuration is done in a way that Akka picks up 127.0.0.1 then you should set this property to `false` and provide a valid hostname in property `akka.remote.netty.tcp.hostname` within your config file.
- set `spark.master` property to use port `6066` instead of `7077`
- Akka port should not be random and the property `akka.remote.netty.tcp.port` should be set to a valid port e.g. 2552.
- `spark.jobserver.kill-context-on-supervisor-down` should be `false`. Since SJS HTTP JVM can be restarted and can connect back to the contexts, we disable the zombie killing logic.

