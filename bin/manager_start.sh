#!/bin/bash
# Script to start the job manager
# args: <job manager actor name> <cluster address>
set -e

. $appdir/setenv.sh

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

# exec java -cp $CLASSPATH $GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $MAIN $1 $2 $conffile 2>&1 &
