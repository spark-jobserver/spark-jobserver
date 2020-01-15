#!/usr/bin/env bash
set -e
curl -L -o /tmp/spark.tgz http://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
tar -xvzf /tmp/spark.tgz -C /tmp
