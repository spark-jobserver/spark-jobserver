#!/bin/bash
# Script to start the job manager
# args: <master> <deployMode> <workDir> <akkaAdress> [<proxyUser>]
set -e

get_abs_script_path() {
   pushd . >/dev/null
   cd $(dirname $0)
   appdir=$(pwd)
   popd  >/dev/null
}
get_abs_script_path

. $appdir/setenv.sh

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

MAIN="spark.jobserver.JobManager"

if [ $2 == "cluster" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS
    --master $1 --deploy-mode cluster
    --files $appdir/log4j-cluster.properties,$3/context.conf,$conffile"
  LOGGING_OPTS="-Dlog4j.configuration=log4j-cluster.properties"
else # client mode
  # Override logging options to provide per-context logging
  LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties -DLOG_DIR=$3"
  GC_OPTS="$GC_OPTS -Xloggc:$3/gc.out"
fi

if [ -n "$REMOTE_JOBSERVER_DIR" ]; then
  appdir=$REMOTE_JOBSERVER_DIR
fi

if [ -n "$5" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --proxy-user $5"
fi

if [ -n "$JOBSERVER_KEYTAB" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --keytab $JOBSERVER_KEYTAB"
fi

cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
      --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
      $SPARK_SUBMIT_OPTIONS
      --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $SPARK_SUBMIT_JAVA_OPTIONS"
      $appdir/spark-job-server.jar $2 $3 $4 $conffile'

mkdir -p "$3"
eval $cmd >"$3/spark-job-server.out" 2>&1 </dev/null &
