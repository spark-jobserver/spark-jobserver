#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/../config/user-ec2-settings.sh

"$bin"/../spark-1.5.0-bin-hadoop2.6/ec2/spark-ec2 destroy $CLUSTER_NAME