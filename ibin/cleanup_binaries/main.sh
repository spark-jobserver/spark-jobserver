#!/usr/bin/env bash

display_help() {
    echo "Usage: `basename $0` -j jumpbox_name -z zookeeper_ip"
}

JUMPBOX_NAME=""
JUMPBOX_IP=""
ZOOKEEPER_IP=""

CF_SETUP=`cf env spark-service | grep FAILED`
if [[ "$CF_SETUP" ]]
then
    echo "ERROR: You need to be logged into your CF environment!"
    exit 1
fi

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -j|--jumpbox)
    JUMPBOX_NAME="$2"
    shift
    shift
    ;;
    -z|--zookeeper)
    ZOOKEEPER_IP="$2"
    shift
    shift
    ;;
    -h|--help)
    display_help
    exit 0
    ;;
    *)
    echo "ERROR: Unknown script parameters"
    display_help
    exit 1
    ;;
esac
done


if [[ ${JUMPBOX_NAME} == "" || ${ZOOKEEPER_IP} == "" ]]
then
    echo "ERROR: Jumpbox name and zookeeper ip required!"
    exit 1
fi

printf "ACTION REQUIRED!\nPlease open another terminal and make a tunnel to Zookeeper. Use the following command for it:\n\n"
printf "./tunnel-to-zookeeper.sh -j $JUMPBOX_NAME -z $ZOOKEEPER_IP\n\n"
sleep 5
read -p "Is tunnel opened (y/n)?" choice
case "$choice" in
  y|Y )
  echo "Good job!:)";;
  n|N )
  echo "No? Can't proceed without opened tunnel. If you have problems opening the tunnel, please contact Jobserver team."
  exit 0
  ;;
  * )
  echo "invalid"
  exit 1
  ;;
esac


export ZK_BINARIES_FILE="$JUMPBOX_NAME-$ZOOKEEPER_IP-zookeeper-libs.txt"
export CF_BINARIES_FILE="$JUMPBOX_NAME-known-libraries.txt"
export BINARIES_TO_DELETE="$JUMPBOX_NAME-$ZOOKEEPER_IP-to-delete.txt"

find_files_to_delete() {
amm zookeeper.sc --zkBinariesFileToSave ${ZK_BINARIES_FILE}
./postgresql.sh -f ${CF_BINARIES_FILE}
amm zookeeper.sc --zkBinariesFileToSave ${ZK_BINARIES_FILE} --cfBinariesFile ${CF_BINARIES_FILE} --binariesToDeleteFile ${BINARIES_TO_DELETE}
}

trap find_files_to_delete EXIT

printf "\nACTION REQUIRED!\nFound `cat ${BINARIES_TO_DELETE} | wc -l` binaries to delete.\n"
printf "Please review the following files carefully:\nCloudFoundary binaries: $CF_BINARIES_FILE\n"
printf "Zookeeper $ZOOKEEPER_IP binaries: $ZK_BINARIES_FILE\nBinaries to delete: $BINARIES_TO_DELETE\n\n"
sleep 10
read -p "Do you want to proceed (y/n)?" choice
case "$choice" in
  y|Y )
  echo "yes"
  amm zookeeper.sc --binariesToDeleteFile ${BINARIES_TO_DELETE}
  ;;
  n|N )
  echo "no. exiting the script (not deleting files)"
  exit 0
  ;;
  * )
  echo "invalid"
  exit 1
  ;;
esac
