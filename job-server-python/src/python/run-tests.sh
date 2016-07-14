#!/usr/bin/env bash
PYTHONPATH=.:$SPARK_HOME/python:$PYTHONPATH python test/apitests.py
#This sleep is here so that all of Spark's shutdown stdout if written before we exit,
#so that we return cleanly to the command prompt.
sleep 2
