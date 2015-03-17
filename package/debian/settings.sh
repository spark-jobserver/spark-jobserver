#!/usr/bin/env bash
INSTALL_DIR=/usr/lib/spark-job-server/
LOG_DIR=/var/log/spark-job-server
PIDFILE=/var/run/spark-jobserver.pid

# For DataStax Enterprise with Spark enabled
SPARK_HOME=/usr/share/dse/spark
SPARK_CONF_HOME=/etc/dse/spark

# Only needed for Mesos deploys
# SPARK_EXECUTOR_URI=/home/spark/spark-0.8.0.tar.gz
# Only needed for YARN running outside of the cluster
# You will need to COPY these files from your cluster to the remote machine
# Normally these are kept on the cluster in /etc/hadoop/conf
# YARN_CONF_DIR=/pathToRemoteConf/conf
