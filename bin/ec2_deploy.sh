#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/../config/user-ec2-settings.sh

#get spark deployment scripts if they haven't been downloaded and extracted yet
SPARK_DIR=ec2Cluster
if [ ! -d "$bin"/../$SPARK_DIR ]; then
    mkdir "$bin"/../$SPARK_DIR
    mkdir -p "$bin"/../$SPARK_DIR/deploy.generic/root/spark-ec2
    wget -P "$bin"/../$SPARK_DIR/deploy.generic/root/spark-ec2 https://raw.githubusercontent.com/amplab/spark-ec2/branch-1.6/deploy.generic/root/spark-ec2/ec2-variables.sh
    wget -P "$bin"/../$SPARK_DIR https://raw.githubusercontent.com/amplab/spark-ec2/branch-1.6/spark_ec2.py
    wget -P "$bin"/../$SPARK_DIR https://raw.githubusercontent.com/amplab/spark-ec2/branch-1.6/spark-ec2
    chmod u+x "$bin"/../$SPARK_DIR/*
fi

#run spark-ec2 to start ec2 cluster
EC2DEPLOY="$bin"/../$SPARK_DIR/spark-ec2
"$EC2DEPLOY" --copy-aws-credentials --key-pair=$KEY_PAIR --hadoop-major-version=yarn --identity-file=$SSH_KEY --region=us-east-1 --zone=us-east-1a --spark-version=$SPARK_VERSION --instance-type=$INSTANCE_TYPE --slaves $NUM_SLAVES launch $CLUSTER_NAME
#There is only 1 deploy host. However, the variable is plural as that is how Spark Job Server named it.
#To minimize changes, I left the variable name alone.
export DEPLOY_HOSTS=$("$EC2DEPLOY" get-master $CLUSTER_NAME | tail -n1)

#This line is a hack to edit the ec2.conf file so that the master option is correct. Since we are allowing Amazon to
#dynamically allocate a url for the master node, we must update the configuration file in between cluster startup
#and Job Server deployment
cp "$bin"/../config/ec2.conf.template "$bin"/../config/ec2.conf
sed -i -E "s/master = .*/master = \"spark:\/\/$DEPLOY_HOSTS:7077\"/g" "$bin"/../config/ec2.conf

#also get ec2_example.sh right
cp "$bin"/ec2_example.sh.template "$bin"/ec2_example.sh
sed -i -E "s/DEPLOY_HOSTS=.*/DEPLOY_HOSTS=\"$DEPLOY_HOSTS:8090\"/g" "$bin"/ec2_example.sh

#open all ports so the master for Spark Job Server to work and you can see the results of your jobs
aws ec2 authorize-security-group-ingress --group-name $CLUSTER_NAME-master --protocol tcp --port 0-65535 --cidr 0.0.0.0/0

cd "$bin"/..
bin/server_deploy.sh ec2
ssh -o StrictHostKeyChecking=no -i "$SSH_KEY"  root@$DEPLOY_HOSTS "(echo 'export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID' >> spark/conf/spark-env.sh)"
ssh -o StrictHostKeyChecking=no -i "$SSH_KEY"  root@$DEPLOY_HOSTS "(echo 'export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY' >> spark/conf/spark-env.sh)"
ssh -o StrictHostKeyChecking=no -i "$SSH_KEY"  root@$DEPLOY_HOSTS "(cd job-server; nohup ./server_start.sh < /dev/null &> /dev/null &)"
echo "The Job Server is listening at $DEPLOY_HOSTS:8090"
