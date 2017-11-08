#!/usr/bin/env bash
set -e
curl -L -o /tmp/spark.tgz http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
tar -xvzf /tmp/spark.tgz -C /tmp
