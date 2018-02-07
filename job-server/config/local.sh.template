#!/usr/bin/env bash

# Environment and deploy file
# For use with bin/server_deploy, bin/server_package etc.
DEPLOY_HOSTS="hostname1.net
              hostname2.net"

APP_USER=spark
APP_GROUP=spark
JMX_PORT=9999
# optional SSH Key to login to deploy server
#SSH_KEY=/path/to/keyfile.pem
INSTALL_DIR=/home/spark/job-server
LOG_DIR=/var/log/job-server
PIDFILE=spark-jobserver.pid
JOBSERVER_MEMORY=1G
SPARK_VERSION=1.6.0
MAX_DIRECT_MEMORY=512M
SPARK_HOME=/home/spark/spark-1.6.0
SPARK_CONF_DIR=$SPARK_HOME/conf
# Only needed for Mesos deploys
SPARK_EXECUTOR_URI=/home/spark/spark-1.6.0.tar.gz
# Only needed for YARN running outside of the cluster
# You will need to COPY these files from your cluster to the remote machine
# Normally these are kept on the cluster in /etc/hadoop/conf
# YARN_CONF_DIR=/pathToRemoteConf/conf
# HADOOP_CONF_DIR=/pathToRemoteConf/conf
#
# Also optional: extra JVM args for spark-submit
# export SPARK_SUBMIT_OPTS+="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5433"
SCALA_VERSION=2.10.4 # or 2.11.6

# optional:
# REMOTE_JOBSERVER_DIR=file://... or hdfs://...

MANAGER_JAR_FILE="$appdir/spark-job-server.jar"
MANAGER_CONF_FILE="$conffile"
MANAGER_EXTRA_JAVA_OPTIONS=
MANAGER_EXTRA_SPARK_CONFS=
MANAGER_LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties"
SPARK_LAUNCHER_VERBOSE=0

# For mesos/yarn use the following variables
# MANAGER_JAR_FILE="$appdir/spark-job-server.jar"
# MANAGER_CONF_FILE="$(basename $conffile)"
# MANAGER_EXTRA_JAVA_OPTIONS=
# MANAGER_EXTRA_SPARK_CONFS="spark.yarn.submit.waitAppCompletion=false|spark.files=$appdir/log4jcluster.properties,$conffile"
# MANAGER_LOGGING_OPTS="-Dlog4j.configuration=log4j-cluster.properties"
