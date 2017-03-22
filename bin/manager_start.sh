#!/bin/bash
# Script to start the job manager
# args: <work dir for context> <cluster address> [proxy_user]
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
              -DLOG_DIR=$2"

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$appdir/gc.out
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

MAIN="spark.jobserver.JobManager"

MESOS_OPTS=""
if [ $1 == "mesos-cluster" ]; then
    MESOS_OPTS="--master $MESOS_SPARK_DISPATCHER --deploy-mode cluster"
    appdir=$REMOTE_JOBSERVER_DIR
fi

if [ ! -z $5 ]; then
  cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
  --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
  --proxy-user $5
  $MESOS_OPTS
  --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES"
  $appdir/spark-job-server.jar $2 $3 $4 $conffile'
else
  cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
  --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
  --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES"
  $MESOS_OPTS
  $appdir/spark-job-server.jar $2 $3 $4 $conffile'
fi

eval $cmd > /dev/null 2>&1 &
# exec java -cp $CLASSPATH $GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $MAIN $1 $2 $conffile 2>&1 &
