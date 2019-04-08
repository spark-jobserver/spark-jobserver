#!/usr/bin/env bash
# Script to start the job server
# Extra arguments will be spark-submit options, for example
#  ./server_start.sh --jars cassandra-spark-connector.jar
#
# Environment vars (note settings.sh overrides):
#   JOBSERVER_MEMORY - defaults to 1G, the amount of memory (eg 512m, 2G) to give to job server
#   JOBSERVER_CONFIG - alternate configuration file to use
#   JOBSERVER_FG    - launches job server in foreground; defaults to forking in background
set -e

get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

set -a
. $appdir/setenv.sh
set +a

GC_OPTS_SERVER="$GC_OPTS_BASE -Xloggc:$appdir/$GC_OUT_FILE_NAME"

MAIN="spark.jobserver.JobServer"


# Setup files for 2nd jobserver
SJS1_CONF_FILE=$conffile
ENV_NAME=$(basename ${conffile})
ENV_NAME=${ENV_NAME//.conf}
SJS2_CONF_NAME=${ENV_NAME}_8091.conf
SJS2_CONF_FILE=$appdir/$SJS2_CONF_NAME
cp -f $SJS1_CONF_FILE $SJS2_CONF_FILE

# Update ports
sed -i '' 's/8090/8091/' $SJS2_CONF_FILE
sed -i '' 's/2552/2553/' $SJS2_CONF_FILE

function stop_jobserver {
    PID_FILE=$1
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE"); then
       echo 'Job server is already running'
       exit 1
    fi
}

function start_jobserver {
    CONF_FILE=$1
    PID_FILE=$2
    LOG_DIR=$3

    unset JAVA_OPTS_SERVER
    JAVA_OPTS_SERVER=$JAVA_OPTS_BASE
    unset LOGGING_OPTS

    LOGGING_OPTS="$MANAGER_LOGGING_OPTS -DLOG_DIR=$LOG_DIR"

    # Note: Removed $@, it is not needed
    cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
      --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
      --driver-java-options "$GC_OPTS_SERVER $JAVA_OPTS_SERVER $LOGGING_OPTS $CONFIG_OVERRIDES"
      $appdir/spark-job-server.jar $CONF_FILE'

    if [ -z "$JOBSERVER_FG" ]; then
      eval $cmd > $LOG_DIR/server_start.log 2>&1 < /dev/null &
      echo $! > $PID_FILE
    else
      eval $cmd
    fi
}

# Setup Logging directories
SJS1_LOG_DIR=${LOG_DIR}_sjs1
SJS2_LOG_DIR=${LOG_DIR}_sjs2
mkdir -p $SJS1_LOG_DIR
mkdir -p $SJS2_LOG_DIR

# SJS PID NAMES
SJS1_PIDFILE=$appdir/spark-jobserver_sjs1.pid
SJS2_PIDFILE=$appdir/spark-jobserver_sjs2.pid

# Stop all running jobserver instances
stop_jobserver $SJS1_PIDFILE
stop_jobserver $SJS2_PIDFILE

# Start all Jobservers
start_jobserver $SJS1_CONF_FILE $SJS1_PIDFILE $SJS1_LOG_DIR
start_jobserver $SJS2_CONF_FILE $SJS2_PIDFILE $SJS2_LOG_DIR
