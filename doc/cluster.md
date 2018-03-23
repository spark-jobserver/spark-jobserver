<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Configuring Job Server for YARN cluster mode](#configuring-job-server-for-yarn-cluster-mode)
  - [Job Server configuration](#job-server-configuration)
  - [Reading files uploaded via frontend](#reading-files-uploaded-via-frontend)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Configuring Job Server for YARN cluster mode

See also running [YARN in client mode](yarn.md), running [YARN on EMR](EMR.md) and running on [Mesos](mesos.md).

### Job Server configuration

Add the following properties in your job server config file:
- set `spark.master` property to `yarn`, `spark://...` or `mesos://...`
- set `spark.submit.deployMode` property to `cluster`
- set `spark.jobserver.context-per-jvm` to `true`
- set `akka.remote.netty.tcp.hostname` to the cluster interface of the host running the frontend
- set `akka.remote.netty.tcp.maximum-frame-size` to support big remote jars fetch

Optional / required in spark standalone mode:
- set `REMOTE_JOBSERVER_DIR` to `hdfs://...`, `file://...` or `http://...` in your settings `xxx.sh`
- copy `spark-job-server.jar`, your job server config and `log4j-cluster.properties` file into this location

Example job server config (replace `CLUSTER-IP` with the internal IP of the host running the job server frontend):

    spark {
      # deploy in yarn cluster mode
      master = yarn
      submit.deployMode = cluster

      jobserver {
        context-per-jvm = true

        # start a H2 DB server, reachable in your cluster
        sqldao {
          jdbc {
            url = "jdbc:h2:tcp://CLUSTER-IP:9092/h2-db;AUTO_RECONNECT=TRUE"
          }
        }
        startH2Server = false
      }
    }

    # start akka on this interface, reachable from your cluster
    akka {
      remote.netty.tcp {
        hostname = "CLUSTER-IP"

        # This controls the maximum message size, including job results, that can be sent
        maximum-frame-size = 100 MiB
      }
    }

Note:
- YARN transfers the files provided via `--files` submit option into the cluster / container. Spark standalone does not support this in cluster mode and you have to transfer them manual.
- Instead of running a H2 DB instance you can also run a real DB reachable inside your cluster. You can't use the default (host only) H2 configuration in a cluster setup.
- Akka binds by [default](../job-server/src/main/resources/application.conf) to the local host interface and is not reachable from the cluster. You need to configure the akka hostname to the cluster internal address.
- At least one slave node needs to be attached to the master for contexts to be successfully created in Cluster Mode.

### Reading files uploaded via frontend

Files uploaded via the data API (`/data`) are stored on your job server frontend host.
Call the [DataFileCache](../job-server-api/src/main/scala/spark/jobserver/api/SparkJobBase.scala) API implemented by the job environment in your spark jobs to access them:

```scala
  object RemoteDriverExample extends NewSparkJob {
    def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
      runtime.getDataFile(...)
```

The job server transfers the files via akka to the host running your driver and caches them there.

Note: Files uploaded via the JAR or binary API are stored and transfered via the Job DB.
