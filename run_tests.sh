#!/usr/bin/env bash

set -e

echo "Running sbt test and coverage report"
sbt clean coverage testPython test coverageReport
echo "Running pycodestyle over .py files"
find job-server-python/src/python -name *.py -exec $HOME/.local/bin/pycodestyle {} +

# report results
echo "Publishing code coverage report codecov.io"
bash <(curl -s https://codecov.io/bash)
