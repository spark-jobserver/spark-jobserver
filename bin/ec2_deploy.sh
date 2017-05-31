#!/bin/bash -eu
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

[ -f "$bin"/../config/user-ec2-settings.sh ] && . "$bin"/../config/user-ec2-settings.sh

#get spark deployment scripts if they haven't been downloaded and extracted yet
SPARK_DIR="$bin"/../ec2Cluster
if [ ! -d "$SPARK_DIR" ]; then
    wget -P "$SPARK_DIR"/deploy.generic/root/spark-ec2 https://raw.githubusercontent.com/amplab/spark-ec2/$SPARK_EC2_BRANCH/deploy.generic/root/spark-ec2/ec2-variables.sh
    wget -P "$SPARK_DIR" https://raw.githubusercontent.com/amplab/spark-ec2/$SPARK_EC2_BRANCH/spark_ec2.py
    wget -P "$SPARK_DIR" https://raw.githubusercontent.com/amplab/spark-ec2/$SPARK_EC2_BRANCH/spark-ec2
    chmod u+x "$SPARK_DIR"/*
fi

#run spark-ec2 to start ec2 cluster
EC2DEPLOY="$SPARK_DIR"/spark-ec2
if [ -n "$VPC_ID" ]; then
   VPC_OPTION="--vpc-id $VPC_ID"
fi
"$EC2DEPLOY" --copy-aws-credentials --key-pair=$KEY_PAIR --hadoop-major-version=yarn --identity-file=$SSH_KEY --region=us-east-1 --zone=$ZONE --spark-version=$SPARK_VERSION --instance-type=$INSTANCE_TYPE --slaves $NUM_SLAVES $VPC_OPTION $SPARK_EC2_OPTIONS launch $CLUSTER_NAME
#There is only 1 deploy host. However, the variable is plural as that is how Spark Job Server named it.
#To minimize changes, I left the variable name alone.
export DEPLOY_HOSTS=$("$EC2DEPLOY" $SPARK_EC2_OPTIONS get-master $CLUSTER_NAME | tail -n1)

#This line is a hack to edit the ec2.conf file so that the master option is correct. Since we are allowing Amazon to
#dynamically allocate a url for the master node, we must update the configuration file in between cluster startup
#and Job Server deployment
cp "$bin"/../config/ec2.conf.template "$bin"/../config/ec2.conf
sed -i -E "s/master = .*/master = \"spark:\/\/$DEPLOY_HOSTS:7077\"/g" "$bin"/../config/ec2.conf

#also get ec2_example.sh right
cp "$bin"/ec2_example.sh.template "$bin"/ec2_example.sh
sed -i -E "s/DEPLOY_HOSTS=.*/DEPLOY_HOSTS=\"$DEPLOY_HOSTS:8090\"/g" "$bin"/ec2_example.sh

#open all ports so the master for Spark Job Server will work and you can see the results of your jobs
#if the security group is in a VPC it needs to be identified by group ID
if [ -n "$VPC_ID" ]; then
   GROUP_ID=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values=$VPC_ID Name=group-name,Values=$CLUSTER_NAME-master | jq -r ".SecurityGroups[0].GroupId")
   GROUP_SELECTOR="--group-id $GROUP_ID"
else
   GROUP_SELECTOR="--group-name $CLUSTER_NAME-master"
fi
if ! ERROR=$(aws ec2 authorize-security-group-ingress $GROUP_SELECTOR --protocol tcp --port 0-65535 --cidr 0.0.0.0/0 2>&1); then
   #aws ec2 fails with "InvalidPermission.Duplicate" if the ports are already open
   if ! grep -Fq InvalidPermission.Duplicate <<< "$ERROR"; then
      #report any other error and fail
      >&2 echo "$ERROR"
      exit 1
   fi
fi

cd "$bin"/..
bin/server_deploy.sh ec2
ssh -o StrictHostKeyChecking=no -i "$SSH_KEY"  root@$DEPLOY_HOSTS "(echo 'export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID' >> spark/conf/spark-env.sh)"
ssh -o StrictHostKeyChecking=no -i "$SSH_KEY"  root@$DEPLOY_HOSTS "(echo 'export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY' >> spark/conf/spark-env.sh)"
ssh -o StrictHostKeyChecking=no -i "$SSH_KEY"  root@$DEPLOY_HOSTS "(cd job-server; nohup ./server_start.sh < /dev/null &> /dev/null &)"
echo "The Job Server is listening at $DEPLOY_HOSTS:8090"
