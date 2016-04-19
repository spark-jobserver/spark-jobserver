#!/bin/bash

#it is to kill the process where is always run in the backgroud ,when the spark job server is down
PNAME=`ps -ef| grep class |grep spark.jobserver.JobManager | awk '{printf("%s\n",$2)}'  `
PIDS=''

if [ "$PNAME" = "" ] ;then
	#echo "not find spark job server process for spark context "
	exit 0
else
	#echo "find spark job server  process  for context:$PNAME"
	for PID in $PNAME
	do
		if [ "$PID" -gt 0 ] 2>/dev/null ;then
		PIDS="$PID $PIDS"
		fi
	done
fi

for PID in $PIDS
do	
	kill $PID
done




