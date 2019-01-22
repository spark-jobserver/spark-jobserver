<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Setting Up The EC2 Cluster](#setting-up-the-ec2-cluster)
- [Using The Example](#using-the-example)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Setting Up The EC2 Cluster

1. Sign up for an Amazon AWS account.
2. Assign your access key ID and secret access key to the bash variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    * I recommend doing this by placing the following export statements in your .bashrc file.
    * export AWS_ACCESS_KEY_ID=accesskeyId
    * export AWS_SECRET_ACCESS_KEY=secretAccessKey
3. Copy job-server/config/user-ec2-settings.sh.template to job-server/config/user-ec2-settings.sh and configure it. In particular, set KEY_PAIR to the name of your EC2 key pair and SSH_KEY to the location of the pair's private key.
    * I recommend using an ssh key that does not require entering a password on every use. Otherwise, you will need to enter the password many times.
4. Run bin/ec2_deploy.sh to start the EC2 cluster. Go to the url printed at the end of the script to view the Spark Job Server frontend. Change the port from 8090 to 8080 to view the Spark Standalone Cluster frontend.
5. Run bin/ec2_example.sh to setup the example. Go to the url printed at the end of the script to view the example.
4. Run bin/ec2_destroy.sh to shutdown the EC2 cluster.

Note: To change the version of Spark on the cluster, set the SPARK_VERSION variable in both config/ec2.sh and config/user-ec2-settings.sh.template. 

Note: The spark-ec2 script is unreliable. It may hang sometimes as it waits for every server in the cluster to come online. If you get an error message like "Warning: SSH connection error. (This could be temporary.)" for 20-30 min, just kill the script, run bin/ec2_destory.sh to kill your cluster, and restart the deploy with bin/ec2_deploy.sh.

## Using The Example

1. Start a Spark Context by pressing the "Start Context" button.
2. Load data by pressing the "Resample" button. The matrix of scatterplots and category selection dropdown will only appear after loading data from the server.
    * It will take approximately 30-35 minutes the first time you press resample after starting a new context. The cluster spends 20 minutes pulling data from an S3 bucket. It spends the rest of the time running the k-means clustering algorithm.
    * Subsequent presses will refresh the data in the scatterplots. These presses will take about 10 seconds as the data is reloaded from memory using a NamedRDD.
3. After performing the data analysis, shutdown the context by pressing the "Stop Context" button.
