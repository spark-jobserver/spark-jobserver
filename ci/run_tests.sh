#!/usr/bin/env bash

set -e

if [ -z "$1" ]
then
  spark_version=$SPARK_VERSION
else
  spark_version=$1
fi
export SPARK_HOME="/opt/spark-${spark_version}-bin-hadoop2.7"
export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"

echo "Running sbt test and coverage report for Spark ${spark_version} at $SPARK_HOME"
sbt clean coverage testPython test coverageReport
echo "Running pycodestyle over .py files"
find job-server-python/src/python -name *.py -exec $HOME/.local/bin/pycodestyle {} +

# report results
echo "Publishing code coverage report codecov.io"
bash <(curl -s https://codecov.io/bash)
