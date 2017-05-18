## Configuring Job Server for Mesos

### Mesos client mode

Configuring job-server for Mesos cluster mode is straight forward. All you need to change is `spark.master` config to 
point to Mesos master URL in job-server config file.

Example config file (important settings are marked with # important):

    spark {
      master =  <mesos master URL here> # important, example: mesos://mesos-master:5050
    
      # Default # of CPUs for jobs to use for Spark standalone cluster
      job-number-cpus = 4

      jobserver {
        port = 8090
        jobdao = spark.jobserver.io.JobSqlDAO

        sqldao {
          # Directory where default H2 driver stores its data. Only needed for H2.
          rootdir = /database

          # Full JDBC URL / init string.  Sorry, needs to match above.
          # Substitutions may be used to launch job-server, but leave it out here in the default or tests won't pass
          jdbc.url = "jdbc:h2:file:/database/h2-db"
        }
      }

      # universal context configuration.  These settings can be overridden, see README.md
      context-settings {
        num-cpu-cores = 2           # Number of cores to allocate.  Required.
        memory-per-node = 512m         # Executor memory per node, -Xmx style eg 512m, #1G, etc.
      }
    }

### Mesos cluster Mode

Configuring job-server for Mesos cluster mode is a bit tricky as compared to client mode.

Here is the checklist for the changes needed for the same:

- You need to start Mesos dispatcher in your cluster by running `./sbin/start-mesos-dispatcher.sh` available in 
spark package. This step is not specific to job-server and as mentioned in [official spark documentation](https://spark.apache.org/docs/latest/running-on-mesos.html#cluster-mode) this is needed 
to submit spark job in Mesos cluster mode. 

- Add following config at the end of job-server's settings.sh file:
    
    ```
    REMOTE_JOBSERVER_DIR=<path to job-server directory> # copy job-server directory on this location on all mesos agent nodes 
    MESOS_SPARK_DISPATCHER=<mesos dispatcher URL> # example: mesos://mesos-dispatcher:7077
    ```

- Set `spark.jobserver.driver-mode` property to `mesos-cluster` in job-server config file.

- Also override akka default configs in job-server config file to support big remote jars fetch, we have to set frame 
size to some large value, for example:

```
akka.remote.netty.tcp {
    # use remote IP address to form akka cluster, not 127.0.0.1. This should be the IP of of the machine where the file 
    # resides. That means for each mesos agents (where job-server directory is copied on REMOTE_JOBSERVER_DIR path),
    # the hostname should be the remote IP of that node.  
    #  
    hostname = "xxxxx" 
    # This controls the maximum message size, including job results, that can be sent
    maximum-frame-size = 104857600b
}
```

- set `spark.master` to Mesos master URL (and not mesos-dispatcher URL).   

- set `spark.jobserver.context-per-jvm` to `true` in job-server config file.

Example config file (important settings are marked with # important):

    spark {
      master =  <mesos master URL here> # important, example: mesos://mesos-master:5050
    
      # Default # of CPUs for jobs to use for Spark standalone cluster
      job-number-cpus = 4

      jobserver {
        port = 8090
        driver-mode = mesos-cluster  #important
        context-per-jvm = true       #important
        jobdao = spark.jobserver.io.JobSqlDAO
        
        sqldao {
          # Directory where default H2 driver stores its data. Only needed for H2.
          rootdir = /database

          # Full JDBC URL / init string.  Sorry, needs to match above.
          # Substitutions may be used to launch job-server, but leave it out here in the default or tests won't pass
          jdbc.url = "jdbc:h2:file:/database/h2-db"
        }
      }

      # universal context configuration.  These settings can be overridden, see README.md
      context-settings {
        num-cpu-cores = 2           # Number of cores to allocate.  Required.
        memory-per-node = 512m         # Executor memory per node, -Xmx style eg 512m, #1G, etc.
      }
    }
    
    akka.remote.netty.tcp {    
        # use remote IP address to form akka cluster, not 127.0.0.1. This should be the IP of of the machine where the file 
        # resides. That means for each mesos agents (where job-server directory is copied on REMOTE_JOBSERVER_DIR path),
        # the hostname should be the remote IP of that node.  
        #  
        hostname = "xxxxx"    #important
        # This controls the maximum message size, including job results, that can be sent
        maximum-frame-size = 104857600b    #important
    }
