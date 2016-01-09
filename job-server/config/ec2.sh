# Environment and deploy file
# For use with bin/server_deploy, bin/server_package etc.
#only look for this file if on local computer, not deployment server, as it won't be there and isn't needed
if [ -a "$bin"/../config/user-ec2-settings.sh ]; then
    . "$bin"/../config/user-ec2-settings.sh
fi
APP_USER=root
APP_GROUP=root
INSTALL_DIR=/root/job-server
LOG_DIR=/var/log/job-server
PIDFILE=spark-jobserver.pid
JOBSERVER_MEMORY=1G
SPARK_VERSION=1.6.0
SPARK_HOME=/root/spark
SPARK_CONF_DIR=$SPARK_HOME/conf
# Only needed for Mesos deploys
SPARK_EXECUTOR_URI=/home/spark/spark-0.8.0.tar.gz
# Only needed for YARN running outside of the cluster
# You will need to COPY these files from your cluster to the remote machine
# Normally these are kept on the cluster in /etc/hadoop/conf
# YARN_CONF_DIR=/pathToRemoteConf/conf
# HADOOP_CONF_DIR=/pathToRemoteConf/conf
SCALA_VERSION=2.10.4 # or 2.11.6
