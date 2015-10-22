#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/../config/user-ec2-settings.sh

"$bin"/../spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION/spark-ec2 destroy $CLUSTER_NAME