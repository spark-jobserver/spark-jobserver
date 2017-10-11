# Docker environment vars
# NOTE: only static vars not intended to be changed by users should appear here, because
#       this file gets sourced in the middle of server_start.sh, so it will override
#       any env vars set in the docker run command line.
PIDFILE=spark-jobserver.pid
SPARK_HOME=/spark
SPARK_CONF_DIR=$SPARK_HOME/conf
# For Docker, always run start script as foreground
JOBSERVER_FG=1

# Environment and deploy file
# For use with bin/server_deploy, bin/server_package etc.
DEPLOY_HOSTS="localhost
              "

APP_USER=spark
APP_GROUP=spark
JMX_PORT=9999
# optional SSH Key to login to deploy server
#SSH_KEY=/path/to/keyfile.pem
INSTALL_DIR=/home/spark/job-server
LOG_DIR=/var/log/job-server
PIDFILE=spark-jobserver.pid
JOBSERVER_MEMORY=8G
SPARK_VERSION=2.2.0
MAX_DIRECT_MEMORY=8G
SPARK_HOME=/spark
SPARK_CONF_DIR=$SPARK_HOME/conf
# Only needed for Mesos deploys
#SPARK_EXECUTOR_URI=/home/spark/spark-2.2.0.tar.gz
# Only needed for YARN running outside of the cluster
# You will need to COPY these files from your cluster to the remote machine
# Normally these are kept on the cluster in /etc/hadoop/conf
# YARN_CONF_DIR=/pathToRemoteConf/conf
# HADOOP_CONF_DIR=/pathToRemoteConf/conf
#
# Also optional: extra JVM args for spark-submit
# export SPARK_SUBMIT_OPTS+="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5433"
SCALA_VERSION=2.11.11 # or 2.11.6
REMOTE_JOBSERVER_DIR=file://home/spark/job-server #or hdfs://...
#MESOS_SPARK_DISPATCHER=mesos://127.0.0.1:7077

export JAVA_VERSION=8-jdk

