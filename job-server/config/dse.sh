# DataStax Distribution Config
# Environment and deploy file
# For use with bin/server_deploy, bin/server_package etc.

APP_USER=cassandra
APP_GROUP=cassandra

#Check Home Directory
#Relative Tar Location and
#Package location for dse-env.sh to get environment variables
if [ -z "$DSE_ENV" ]; then
    for include in "$HOME/.dse-env.sh" \
                   "`dirname "$0"`/../../../bin/dse-env.sh" \
                   "/etc/dse/dse-env.sh"; do
        if [ -r "$include" ]; then
            DSE_ENV="$include"
            break
        fi
    done
fi

#ENV is set for the build script server_package, If it isn't set then we need
# to be able to read DSE_ENV to set Spark Env variables
if [ -z "$DSE_ENV" ] &&  [ -z "$ENV" ]; then
    echo "DSE_ENV could not be determined."
    exit 1
elif [ -r "$DSE_ENV" ]; then
    . "$DSE_ENV"
elif [ -z "$ENV" ]; then
    echo "Location pointed by DSE_ENV not readable: $DSE_ENV"
    exit 1
fi

SPARK_VERSION=1.4.1.1 #Last digit is DSE Specific 

DEPLOY_HOSTS="localhost"

INSTALL_DIR="$DSE_COMPONENTS_ROOT/spark/spark-jobserver"
LOG_DIR=/var/log/spark/job-server

PIDFILE=spark-jobserver.pid

SPARK_CONF_DIR=${SPARK_CONF_DIR:-"$SPARK_HOME/conf"}

SCALA_VERSION=2.10.5 # or 2.11.6
