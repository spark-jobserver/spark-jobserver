#!/bin/bash
# Script to start the job manager
# args: <work dir for context> <cluster address>
set -e

get_abs_script_path() {
   pushd . >/dev/null
   cd $(dirname $0)
   appdir=$(pwd)
   popd  >/dev/null
}
get_abs_script_path

. $appdir/setenv.sh

# Override logging options to provide per-context logging
LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties
              -DLOG_DIR=$1"

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$appdir/gc.out
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS="-XX:MaxDirectMemorySize=512M
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

MAIN="spark.jobserver.JobManager"

cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
  --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
  --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES"
  $appdir/spark-job-server.jar $1 $2 $conffile'

eval $cmd > /dev/null 2>&1 &
# exec java -cp $CLASSPATH $GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $MAIN $1 $2 $conffile 2>&1 &
