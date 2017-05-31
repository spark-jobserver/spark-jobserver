#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/../config/user-ec2-settings.sh

"$bin"/../ec2Cluster/spark-ec2 destroy $CLUSTER_NAME --delete-groups $SPARK_EC2_OPTIONS
