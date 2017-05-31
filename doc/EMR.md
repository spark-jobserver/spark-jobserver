## Step by step instruction on how to run Spark Job Server on EMR 4.2.0 (Spark 1.6.0)

### Create EMR 4.2.0 cluster

Create EMR cluster using AWS EMR console or aws cli.
The script below creates ten r3.2xlarge slaves cluster.
Replace the following parameters with correct values:
- subnet-xxxxxxx
- us-west-1
- KeyName=spark
```
aws emr create-cluster --name jobserver_test --release-label emr-4.2.0 \
--instance-groups InstanceCount=1,Name=sparkMaster,InstanceGroupType=MASTER,InstanceType=m3.xlarge \
InstanceCount=10,BidPrice=2.99,Name=sparkSlave,InstanceGroupType=CORE,InstanceType=r3.2xlarge \
--applications Name=Hadoop Name=Spark \
--ec2-attributes KeyName=spark,SubnetId=subnet-xxxxxxx --region us-west-1 \
--use-default-roles
```

### Configure master box

1. Ssh to master box using cluster Key file
 ```
 ssh -i <key>.pem hadoop@<master_ip>
 ```

2. Create required folders in /mnt
 ```
 mkdir /mnt/work
 mkdir -p /mnt/lib/.ivy2
 ln -s /mnt/lib/.ivy2 ~/.ivy2
 ```

3. Create spark-jobserver log dirs
 ```
 mkdir /mnt/var/log/spark-jobserver
 ```

### Build spark-jobserver distribution

1. Install sbt 0.13.9 as described here http://www.scala-sbt.org/0.13/tutorial/Manual-Installation.html
2. Install jdk 1.7.0 and git
 ```
 sudo yum install java-1.7.0-openjdk-devel git
 ```
 
3. Clone spark-jobserver and checkout release v0.6.1
 ```
 cd /mnt/work
 git clone https://github.com/spark-jobserver/spark-jobserver.git
 cd spark-jobserver
 git checkout v0.6.1
 ```
 
4. Build spark-jobserver
 ```
 sbt clean update package assembly
 ```

5. Create config/emr.sh
 ```
 APP_USER=hadoop
 APP_GROUP=hadoop
 INSTALL_DIR=/mnt/lib/spark-jobserver
 LOG_DIR=/mnt/var/log/spark-jobserver
 PIDFILE=spark-jobserver.pid
 JOBSERVER_MEMORY=1G
 SPARK_VERSION=1.6.0
 SPARK_HOME=/usr/lib/spark
 SPARK_CONF_DIR=/etc/spark/conf
 HADOOP_CONF_DIR=/etc/hadoop/conf
 YARN_CONF_DIR=/etc/hadoop/conf
 SCALA_VERSION=2.10.5
 ```

6. Create config/emr.conf
 ```
 spark {
   # spark.master will be passed to each job's JobContext
   master = "yarn-client"
   jobserver {
     port = 8090
     # Note: JobFileDAO is deprecated from v0.7.0 because of issues in
     # production and will be removed in future, now defaults to H2 file.
     jobdao = spark.jobserver.io.JobSqlDAO

     filedao {
       rootdir = /mnt/tmp/spark-jobserver/filedao/data
     }
     sqldao {
       # Slick database driver, full classpath
       slick-driver = slick.driver.H2Driver

       # JDBC driver, full classpath
       jdbc-driver = org.h2.Driver

       # Directory where default H2 driver stores its data. Only needed for H2.
       rootdir = /tmp/spark-jobserver/sqldao/data

       # Full JDBC URL / init string, along with username and password.  Sorry, needs to match above.
       # Substitutions may be used to launch job-server, but leave it out here in the default or tests won't pass
       jdbc {
         url = "jdbc:h2:file:/tmp/spark-jobserver/sqldao/data/h2-db"
         user = ""
         password = ""
       }

       # DB connection pool settings
       dbcp {
         enabled = false
         maxactive = 20
         maxidle = 10
         initialsize = 10
       }
     }
   }
   # predefined Spark contexts
   contexts {
     # test {
     #   num-cpu-cores = 1            # Number of cores to allocate.  Required.
     #   memory-per-node = 1g         # Executor memory per node, -Xmx style eg 512m, 1G, etc.
     #   spark.executor.instances = 1
     # }
     # define additional contexts here
   }
   # universal context configuration.  These settings can be overridden, see README.md
   context-settings {
     num-cpu-cores = 4          # Number of cores to allocate.  Required.
     memory-per-node = 8g         # Executor memory per node, -Xmx style eg 512m, #1G, etc.
     spark.executor.instances = 2
     # If you wish to pass any settings directly to the sparkConf as-is, add them here in passthrough,
     # such as hadoop connection settings that don't use the "spark." prefix
     passthrough {
       #es.nodes = "192.1.1.1"
     }
   }
   # This needs to match SPARK_HOME for cluster SparkContexts to be created successfully
   home = "/usr/lib/spark"
 }
 ```

7. Build tar.gz package. The package location will be /tmp/job-server/job-server.tar.gz
 ```
 bin/server_package.sh emr
 ```

### Deploy spark-jobserver
1. Ssh to master box using cluster Key file

2. Create directory for spark-jobserver and extract job-server.tar.gz into it
 ```
 mkdir /mnt/lib/spark-jobserver
 cd /mnt/lib/spark-jobserver
 tar zxf /tmp/job-server/job-server.tar.gz
 ```

3. Check emr.conf and settings.sh

4. Start jobserver
 ```
 ./server_start.sh
 ```

5. Check logs
 ```
 tail -300f /mnt/var/log/spark-jobserver/spark-job-server.log
 ```

6. To stop jobserver run
 ```
 ./server_stop.sh
 ```

### Test spark-jobserver
1. Check current status
 ```
 # check current jars
 curl localhost:8090/jars
 # check current contexts
 curl localhost:8090/contexts
 ```

2. Upload jobserver example jar to testapp
 ```
 curl --data-binary @/mnt/work/spark-jobserver/job-server-tests/target/scala-2.10/job-server-tests_2.10-0.6.1.jar \
 localhost:8090/jars/testapp
 # check current jars. should return testapp
 curl localhost:8090/jars
 ```

3. Create test context
 ```
 # create test context
 curl -d "" 'localhost:8090/contexts/test?num-cpu-cores=1&memory-per-node=512m&spark.executor.instances=1'
 # check current contexts. should return test
 curl localhost:8090/contexts
 ```

4. Run WordCount example
 ```
 # run WordCount example (should be done in 1-2 sec)
 curl -d "input.string = a b c a b see" \
 'localhost:8090/jobs?appName=testapp&classPath=spark.jobserver.WordCountExample&context=test&sync=true'
 ```

5. Check jobs
 ```
 # check jobs
 curl localhost:8090/jobs
 ```
 
6. Check YARN website on port 8088. It should be Spark application RUNNING for test context.

### Troubleshooting
1. If for some reason spark-jobserver can not start Spark Application on Yarn cluster you can try to open Spark Shell and check if it can start Spark Application on Yarn cluster
 ```
 spark-shell \
     --num-executors 1 \
     --driver-memory 1g \
     --executor-memory 1g \
     --executor-cores 1
 ```

2. Check yarn website on port 8088. Spark-shell application should be have RUNNING status

3. Run the following test command in Spark Shell to test spark-shell yarn application
 ```
 sc.parallelize(1 to 10, 1).count
 ```

4. If shell works but spark-jobserver still does not work check logs
 - on jobserver /var/log/spark-jobserver/spark-job-server.log
 - on Cluster: Yarn App logs (website on port 8088)
 - on Cluster: Containers stderr, stdout (website on port 8088)

5. Make sure spark-jobserver uses EMR default spark installed in /usr/lib/spark
