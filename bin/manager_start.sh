#!/bin/bash
# Script to start the job manager
# args: <work dir for context> <cluster address> <spark context name> <spark.driver.memory> [additional parameters]
# additional parameters should look this way: "spark.executor.cores=4|..."
set -e

get_abs_script_path() {
   pushd . >/dev/null
   cd $(dirname $0)
   appdir=$(pwd)
   popd  >/dev/null
}
get_abs_script_path

. $appdir/setenv.sh

WORK_DIR=$1
CLUSTER_ADDRESS=$2
SPARK_CONTEXT_NAME=$3
SPARK_DRIVER_MEMORY=$4
SPARK_CONFIGURATION=$5

# Override logging options to provide per-context logging
LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties
              -DLOG_DIR=$1
              -DSPARK_CONTEXT_NAME=$SPARK_CONTEXT_NAME"

GC_OPTS="-verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$appdir/gc.out
         -XX:MaxPermSize=512m
         -XX:+UseCompressedOops -XX:+PrintGCDetails -XX:+PrintGCDateStamps
         -XX:+UseG1GC -XX:MaxHeapFreeRatio=70"

JAVA_OPTS="-XX:MaxDirectMemorySize=512M
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

EXTRA_CLASSPATH_OPTS="/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/lib/hadoop/hadoop-aws.jar:/home/hadoop/spark-jobserver/libs/*"

MAIN="spark.jobserver.JobManager"

if [ "$SPARK_DRIVER_MEMORY" -eq "0" ]; then
    SPARK_DRIVER_MEMORY=JOBSERVER_MEMORY
fi

cmd="$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $SPARK_DRIVER_MEMORY
  --conf \"spark.executor.extraJavaOptions=$LOGGING_OPTS\"
  --conf \"spark.driver.extraClassPath=$EXTRA_CLASSPATH_OPTS\"
  --conf \"spark.executor.extraClassPath=$EXTRA_CLASSPATH_OPTS\"
  --driver-java-options \"$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES\"
  $appdir/spark-job-server.jar $WORK_DIR $CLUSTER_ADDRESS $SPARK_CONFIGURATION"

eval $cmd > "$WORK_DIR/spark-job-server.log" 2>&1 &
# exec java -cp $CLASSPATH $GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $MAIN $1 $2 $conffile 2>&1 &
