#!/usr/bin/env bash

display_help() {
    echo "Usage: `basename $0` -f file_to_save_known_binaries"
}

KNOWN_BINARIES_FILE=""

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -f|--file)
    KNOWN_BINARIES_FILE="$2"
    shift
    shift
    ;;
    *)
    echo "ERROR: Unknown parameter!"
    display_help
    exit 1
    ;;
esac
done

if [[ $KNOWN_BINARIES_FILE == "" ]]
then
    echo "ERROR: File path to save binaries names is required!"
    display_help
    exit 1
fi


echo "Extracting CF env information to connect to PostgreSQL"
CREDENTIALS_ENV=`cf env spark-service | awk '/System-Provided:/{flag=1;next}/^$/{flag=0}flag' | jq .VCAP_SERVICES.postgresql[].credentials`
DB_NAME=`echo ${CREDENTIALS_ENV} | jq .dbname | tr -d '"'`
DB_HOSTNAME=`echo ${CREDENTIALS_ENV} | jq .hostname | tr -d '"'`
DB_PORT=`echo ${CREDENTIALS_ENV} | jq .port | tr -d '"'`
DB_USERNAME=`echo ${CREDENTIALS_ENV} | jq .username | tr -d '"'`
export PGPASSWORD=`echo ${CREDENTIALS_ENV} | jq .password | tr -d '"'` # export so that child psql process sees it in env

echo "Found connection configuration:"
echo cf ssh spark-service -N -L 127.0.0.1:5432:$DB_HOSTNAME:$DB_PORT
echo psql -h localhost -p 5432 $DB_NAME $DB_USERNAME
echo "Password: $PGPASSWORD"
cf ssh spark-service -N -L 127.0.0.1:5432:$DB_HOSTNAME:$DB_PORT &
SSH_PID=$!
echo "Started a tunnel to DB (pid $SSH_PID). Waiting a bit.."
sleep 5
echo "Retrieving data from db.."
psql -h localhost -p 5432 $DB_NAME $DB_USERNAME -c "SELECT CONCAT('l~', tenant_id, '~', app_id, '~', lib_name) FROM libs;"  > ${KNOWN_BINARIES_FILE}

if [[ SSH_PID == "" ]]
then
    echo "Nothing to cleanup"
else
    echo "Closing ssh tunnel"
    echo "Executing 'kill $SSH_PID'"
    kill $SSH_PID
fi
