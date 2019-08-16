#!/usr/bin/env bash
set -e
curl -L -o /tmp/spark.tgz http://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz
tar -xvzf /tmp/spark.tgz -C /tmp
