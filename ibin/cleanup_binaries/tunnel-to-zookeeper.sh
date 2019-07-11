#!/usr/bin/env bash

display_help() {
    echo "Usage: `basename $0` -j jumpbox_name -z zookeeper_ip"
}

JUMPBOX_IP=""
JUMPBOX_NAME=""
ZOOKEEPER_IP=""
PORT="9999"

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
    echo "ERROR: unknown script parameters!"
    display_help
    exit 1
    ;;
esac
done

if [[ $JUMPBOX_NAME == "" || $ZOOKEEPER_IP == "" ]]
then
    exit 1
fi

case "$JUMPBOX_NAME" in
      dev-amin)
        JUMPBOX_IP="172.18.104.157"
        ;;
      dev-gcp)
        JUMPBOX_IP="104.155.91.162"
        ;;
      dev-azure)
        JUMPBOX_IP="52.136.231.79"
        ;;
      aws-staging)
        JUMPBOX_IP="34.195.139.205"
        ;;
      aws-canary)
        JUMPBOX_IP="52.58.138.97"
        ;;
      aws-eu-live)
        JUMPBOX_IP="52.57.232.171"
        ;;
      aws-us-live)
        JUMPBOX_IP="34.193.149.238"
        ;;
      azure-staging)
        JUMPBOX_IP="51.144.225.27"
        ;;
      azure-live)
        JUMPBOX_IP="40.113.130.114"
        ;;
      gcp-staging)
        JUMPBOX_IP="35.193.214.40"
        ;;
      gcp-live)
        JUMPBOX_IP="35.194.18.23"
        ;;
      sc7-live)
        JUMPBOX_IP="172.18.113.24"
        ;;
      sc7-staging)
        JUMPBOX_IP="172.18.116.109"
        ;;
      *)
        echo "Usage: $0 {dev-amin|dev-gcp|dev-azure|aws-staging|aws-canary|aws-eu-live|aws-us-live|azure-staging|azure-live|gcp-staging|gcp-live|sc7-staging|sc7-live}"
        exit 1
esac

# ssh -L 9998:host2:22 -N host1
echo "Opening tunnel to $ZOOKEEPER_IP Zookeeper DB over Jumpbox $JUMPBOX_IP on local port $PORT"
ssh -o UserKnownHostsFile=/dev/null -L $PORT:$ZOOKEEPER_IP:2181 -N $JUMPBOX_IP
