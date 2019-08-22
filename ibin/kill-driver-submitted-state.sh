#!/bin/bash

if [[ -z "${SPARK_HOME}" ]]; then
    echo "Please set the SPARK_HOME env variable"
    exit 1
fi

if [[ -z "$1" ]]; then
     echo "Usage: ./<this_script>.sh <spark master IP>"
     exit 1
fi

SPARK_MASTER=$1

TMP_FILE_PATH=$(mktemp)
curl http://$SPARK_MASTER:8080/json/ | jq '.activedrivers[] | select(.state=="SUBMITTED") | .id' > $TMP_FILE_PATH

printf "\n\n----------- Drivers in Submitted state ------------\n"
cat $TMP_FILE_PATH
printf "\n----------- End -------------\n\n"

read -e -p "
Trigger driver deletion ? [Y/n] " ANSWER

[[ $ANSWER == "Y" ]] && cat $TMP_FILE_PATH | xargs -I % $SPARK_HOME/bin/spark-class org.apache.spark.deploy.Client kill spark://$SPARK_MASTER:7077 "%"

rm $TMP_FILE_PATH
