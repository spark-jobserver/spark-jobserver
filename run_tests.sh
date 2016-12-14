#!/bin/bash

set -e

sbt clean coverage testPython test coverageReport
find job-server-python/src/python -name *.py -exec pep8 {} +

# report results
bash <(curl -s https://codecov.io/bash)
