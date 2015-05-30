#!/bin/bash

set -e

get_abs_script_path() {
  pushd . >/dev/null
  cd $(dirname $0)
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

conffile=$(ls -1 $appdir/*.conf | head -1)
if [ -z "$conffile" ]; then
  echo "No configuration file found"
  exit 1
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

# Pull in other env vars in spark config, such as MESOS_NATIVE_LIBRARY
. $SPARK_CONF_DIR/spark-env.sh

if [ -z "$LOG_DIR" ]; then
  LOG_DIR=/tmp/job-server
  echo "LOG_DIR empty; logging will go to $LOG_DIR"
fi
mkdir -p $LOG_DIR

LOGGING_OPTS="-Dlog4j.configuration=log4j-server.properties
              -DLOG_DIR=$LOG_DIR"

# For Mesos
CONFIG_OVERRIDES="-Dspark.executor.uri=$SPARK_EXECUTOR_URI "
# For Mesos/Marathon, use the passed-in port
if [ "$PORT" != "" ]; then
  CONFIG_OVERRIDES+="-Dspark.jobserver.port=$PORT "
fi

# This needs to be exported for standalone mode so drivers can connect to the Spark cluster
export SPARK_HOME

# job server jar needs to appear first so its deps take higher priority
# need to explicitly include app dir in classpath so logging configs can be found
CLASSPATH="$appdir:$appdir/spark-job-server.jar:$($SPARK_HOME/bin/compute-classpath.sh)"