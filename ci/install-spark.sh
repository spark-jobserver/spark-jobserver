#!/usr/bin/env bash
set -e
echo "Spark version $SPARK_VERSION and hadoop version $HADOOP_VERSION are going to be installed"
wget http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /tmp
