#!/usr/bin/env bash
set -e

for SPARK_VERSION in 2.4.7 3.0.2 3.1.1
do
  curl -s -L -o /opt/spark.tgz http://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz
  tar -xzf /opt/spark.tgz -C /opt
done