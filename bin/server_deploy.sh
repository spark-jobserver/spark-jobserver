#!/bin/bash
# Script for deploying the job server to a host
ENV=$1
if [ -z "$ENV" ]; then
  echo "Syntax: $0 <Environment>"
  echo "   for a list of environments, ls config/*.sh"
  exit 0
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

if [ -z "$CONFIG_DIR" ]; then
  CONFIG_DIR=`cd "$bin"/../config/; pwd`
fi
configFile="$CONFIG_DIR/$ENV.sh"
if [ ! -f "$configFile" ]; then
  echo "Could not find $configFile"
  exit 1
fi
. $configFile

echo Deploying job server to $DEPLOY_HOSTS...

cd $(dirname $0)/..
sbt ++$SCALA_VERSION job-server/assembly
if [ "$?" != "0" ]; then
  echo "Assembly failed"
  exit 1
fi

FILES="job-server/target/scala-2.10/spark-job-server.jar
       bin/server_start.sh
       bin/server_stop.sh
       $CONFIG_DIR/$ENV.conf
       config/log4j-server.properties"

ssh_key_to_use=""
if [ -n "$SSH_KEY" ]  ; then
  ssh_key_to_use="-i $SSH_KEY"
fi

for host in $DEPLOY_HOSTS; do
  # We assume that the deploy user is APP_USER and has permissions
  ssh $ssh_key_to_use  ${APP_USER}@$host mkdir -p $INSTALL_DIR
  scp $ssh_key_to_use  $FILES ${APP_USER}@$host:$INSTALL_DIR/
  scp $ssh_key_to_use  $configFile ${APP_USER}@$host:$INSTALL_DIR/settings.sh
done
