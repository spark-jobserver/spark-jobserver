#!/bin/bash

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

if [ -z "$SPARK_CONF_DIR" ]; then
  SPARK_CONF_DIR=$SPARK_HOME/conf
fi

if [ -z "$LOG_DIR" ]; then
  LOG_DIR=/tmp/job-server
  echo "LOG_DIR empty; logging will go to $LOG_DIR"
fi
mkdir -p $LOG_DIR

LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties
              -DLOG_DIR=$LOG_DIR"

# For Mesos
CONFIG_OVERRIDES="-Dspark.executor.uri=$SPARK_EXECUTOR_URI "
# For Mesos/Marathon, use the passed-in port
if [ "$PORT" != "" ]; then
  CONFIG_OVERRIDES+="-Dspark.jobserver.port=$PORT "
fi

if [ -z "$JOBSERVER_MEMORY" ]; then
  JOBSERVER_MEMORY=1G
fi

# This needs to be exported for standalone mode so drivers can connect to the Spark cluster
export SPARK_HOME
export YARN_CONF_DIR
export HADOOP_CONF_DIR
