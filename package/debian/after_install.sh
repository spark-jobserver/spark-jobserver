#!/bin/bash
ln -s /opt/spark-job-server/service /etc/init.d/spark-job-server
mkdir -p /var/log/spark-job-server
mkdir -p /var/lib/spark-job-server
