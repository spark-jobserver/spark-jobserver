#!/usr/bin/env bash
PYTHONPATH=.:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH python test/apitests.py
exitCode=$?
#This sleep is here so that all of Spark's shutdown stdout if written before we exit,
#so that we return cleanly to the command prompt.
sleep 2
exit $exitCode
