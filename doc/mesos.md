## Configuring Job Server for Mesos

See also running [YARN in client mode](yarn.md), running [YARN in cluster mode](yarn-cluster.md) and running [YARN on EMR](EMR.md).

### Mesos client mode

Configuring job-server for Mesos client mode is straight forward. All you need to change is `spark.master` config to 
point to Mesos master URL in job-server config file.

Example config file (important settings are marked with # important):

    spark {
      master = <mesos master URL here> # example: mesos://mesos-master:5050
    }

### Mesos cluster mode

Configuring job-server for Mesos cluster mode is a bit tricky as compared to client mode.

You need to start Mesos dispatcher in your cluster by running `./sbin/start-mesos-dispatcher.sh` available in 
spark package. This step is not specific to job-server and as mentioned in [official spark documentation](https://spark.apache.org/docs/latest/running-on-mesos.html#cluster-mode) this is needed 
to submit spark job in Mesos cluster mode. 

Add the following config to you job-server config file:
- set `spark.master` property to messos dispatcher URL (example: `mesos://mesos-dispatcher:7077`)
- set `spark.submit.deployMode` property to `cluster`
- set `spark.jobserver.context-per-jvm` to `true`
- set `akka.remote.netty.tcp.hostname` to the cluster interface of the host running the frontend
- set `akka.remote.netty.tcp.maximum-frame-size` to support big remote jars fetch

Example job server config (replace `CLUSTER-IP` with the internal IP of the host running the job server frontend):

    spark {
      master =  <mesos dispatcher URL> # example: mesos://mesos-dispatcher:7077
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

- Optional: Add following config at the end of job-server's settings.sh file:
    
    ```
    REMOTE_JOBSERVER_DIR=<path to job-server directory> # copy of job-server directory on all mesos agent nodes 
    ```
