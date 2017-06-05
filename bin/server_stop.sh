#!/bin/bash
# Script to stop the job server

get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

if [ -f "$appdir/settings.sh" ]; then
  . "$appdir/settings.sh"
else
  echo "Missing $appdir/settings.sh, exiting"
  exit 1
fi

pidFilePath=$appdir/$PIDFILE
PID="$(cat "$pidFilePath")"

if [ ! -f "$pidFilePath" ] || ! kill -0 "$(cat "$pidFilePath")"; then
   echo 'Job server not running'
else
  echo 'Stopping job server...'
  "$(dirname "$0")"/kill-process-tree.sh 15 $PID && rm "$pidFilePath"
  echo '...job server stopped'
fi

if [ -f "$pidFilePath" ]; then
  echo "Removing PID file, cause there is not such process ${PID}."
  rm "$pidFilePath"
fi
