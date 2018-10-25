#!/usr/bin/env bash
# Script to stop the job server

get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

. $appdir/setenv.sh

pidFilePath=$appdir/$PIDFILE

if [ ! -f "$pidFilePath" ]; then
  echo 'Job server not running'
else
  PID="$(cat "$pidFilePath")"
  if ! kill -0 $PID; then
    echo "PID file exists but the process $PID does not exist. Removing $pidFilePath"
    rm "$pidFilePath"
  else
    echo 'Stopping job server...'
    "$(dirname "$0")"/kill-process-tree.sh 15 $PID
    if ! kill -0 $PID 2> /dev/null ; then
      echo '...job server stopped'
      rm "$pidFilePath"
    else
      echo '...?? job server is still running'
    fi
  fi
fi
