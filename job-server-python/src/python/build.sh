#!/usr/bin/env bash
export SJS_VERSION=$1
PYSPARK_PYTHON=$2

$PYSPARK_PYTHON $3 build --build-base ../../target/python \
egg_info --egg-base ../../target/python \
bdist_egg --bdist-dir /tmp/bdist --dist-dir ../../target/python --skip-build

$PYSPARK_PYTHON $3 build --build-base ../../target/python \
bdist_wheel --bdist-dir /tmp/bdist --dist-dir ../../target/python --skip-build
