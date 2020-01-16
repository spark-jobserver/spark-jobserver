#!/usr/bin/env bash

set -e

get_abs_script_path() {
  pushd . >/dev/null
  cd $(dirname $0)
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

if [ -f "$JOBSERVER_CONFIG" ]; then
  conffile="$JOBSERVER_CONFIG"
else
  conffile=$(ls -1 $appdir/*.conf | head -1)
  if [ -z "$conffile" ]; then
    echo "No configuration file found"
    exit 1
  fi
fi

if [ -f "$appdir/settings.sh" ]; then
  . $appdir/settings.sh
else
  echo "Missing $appdir/settings.sh, exiting"
  exit 1
fi

if [ -z "$SPARK_HOME" ]; then
  echo "Please set SPARK_HOME or put it in $appdir/settings.sh first"
  exit 1
fi

if [ ! -d "$SPARK_HOME" ]; then
  echo "SPARK_HOME doesn't exist: $SPARK_HOME, exiting"
  exit 1
fi

if [ -z "$SPARK_CONF_DIR" ]; then
  SPARK_CONF_DIR=$SPARK_HOME/conf
fi

if [ ! -d "$SPARK_CONF_DIR" ]; then
  echo "SPARK_CONF_DIR doesn't exist: $SPARK_CONF_DIR, exiting"
  exit 1
fi

if [ -z "$LOG_DIR" ]; then
  LOG_DIR=/tmp/job-server
  echo "LOG_DIR empty; logging will go to $LOG_DIR"
fi
mkdir -p $LOG_DIR

LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties
              -DLOG_DIR=$LOG_DIR"

if [ -z "$JMX_PORT" ]; then
    JMX_PORT=9999
    echo "JMX_PORT empty, using default $JMX_PORT"
fi

# For Mesos
CONFIG_OVERRIDES=""
if [ -n "$SPARK_EXECUTOR_URI" ]; then
  CONFIG_OVERRIDES="-Dspark.executor.uri=$SPARK_EXECUTOR_URI "
fi
# For Mesos/Marathon, use the passed-in port
if [ "$PORT" != "" ]; then
  CONFIG_OVERRIDES+="-Dspark.jobserver.port=$PORT "
fi

if [ "$DEBUG_PORT" != "" ]; then
  CONFIG_OVERRIDES+="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=$DEBUG_PORT "
fi

if [ -z "$JOBSERVER_MEMORY" ]; then
  JOBSERVER_MEMORY=1G
fi

if [ -z "$MAX_DIRECT_MEMORY" ]; then
  MAX_DIRECT_MEMORY=512M
fi

# This needs to be exported for standalone mode so drivers can connect to the Spark cluster
export SPARK_HOME
export YARN_CONF_DIR
export HADOOP_CONF_DIR

GC_OPTS_BASE="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:-PrintGCTimeStamps -XX:+PrintGCDateStamps
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS_BASE="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY
         -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

GC_OUT_FILE_NAME="gc.out"

# To truly enable JMX in AWS and other containerized environments, also need to set
# -Djava.rmi.server.hostname equal to the hostname in that environment.  This is specific
# depending on AWS vs GCE etc.
JAVA_OPTS_SERVER="${JAVA_OPTS_BASE} \
          -Dcom.sun.management.jmxremote.port=$JMX_PORT \
          -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
          -Dcom.sun.management.jmxremote.authenticate=false \
          -Dcom.sun.management.jmxremote.ssl=false"

: ${MANAGER_JAR_FILE:="file:$appdir/spark-job-server.jar"}
: ${MANAGER_CONF_FILE:="file:$conffile"}
: ${MANAGER_LOGGING_OPTS:="-Dlog4j.configuration=file:$appdir/log4j-server.properties"}
