#!/bin/bash

LOG_PATH_NAME="$1"

PNAME=`ps -ef|grep spark|grep jobserver  |grep ${LOG_PATH_NAME} | awk '{printf("%s\n",$2)}'  `
PIDS=''

if [ "$PNAME" = "" ] ;then
	#echo "${LOG_PATH_NAME}   is not started"
	exit 0
else
	#echo "find context process :$PNAME"
	for PID in $PNAME
	do
		if [ "$PID" -gt 0 ] 2>/dev/null ;then
		PIDS="$PID $PIDS"
		fi
	done
fi

#echo "begin to kill context process :$PIDS"
for PID in $PIDS
do	
	echo "killed $PID" 
	kill $PID
done

#echo "waiting context process exit:$PIDS"

for PID in $PIDS
do
	for   i in `seq 0 999 `
	do
		#echo "begin waiting context process exit:$PID"
		p=`ps -ef|grep spark |grep jobserver  |grep ${LOG_PATH_NAME} |grep $PID  `
		#echo $p
		if [ "$p" = "" ];then 
			echo "$PID has exit"
			break
		else 
			echo "waitting $PID to exit"
		sleep 1
		fi
	done
done
		
	


