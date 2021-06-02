# Docker

The [Dockerfile](../Dockerfile) in the root directory of this project creates a jobserver container.
It is the baseline for various [docker setups](../docker-setups/) that simplify development, testing and deployment.

## Build
You can build the jobserver container image by invoking
```
docker build -t jobserver .
```
in the project root directory.

### Arguments
The Dockerfile defaults to the following build arguments:
```
SBT_VERSION=1.2.8
SCALA_VERSION=2.11.8
SPARK_VERSION=2.4.5
HADOOP_VERSION=2.7
```
You can override these arguments on `build` with the `--build-arg` parameter:
```
docker build --build-arg SCALA_VERSION=2.12.12 -t jobserver .
```

## Usage
Having built the container image, you can use it by invoking
```
docker run -v <config-folder>:/opt/sparkjobserver/config jobserver
```
(i.e. bind-mount a config folder inside)
assuming `config-folder` is an (absolute) path to a directory containing jobserver configuration files.
You can find examples for configs and config folders under [docker-setups](../docker-setups/).

### Environment variables

The Dockerfile default to the following environment variables:
```
# Jobserver settings
SPARK_JOBSERVER_MEMORY=1G
JOBSERVER_CONF_DIR=/opt/sparkjobserver/config
LOGGING_OPTS="-Dlog4j.configuration=file:/opt/sparkjobserver/config/log4j.properties"
# JobManager settings
MANAGER_JAR_FILE=/opt/sparkjobserver/bin/spark-job-server.jar
MANAGER_CONF_FILE=/opt/sparkjobserver/config/jobserver.conf
```
You can override these arguments on `run` with the `--env` parameter:
```
docker run --env SPARK_JOBSERVER_MEMORY=1G -v <config-folder>:/opt/sparkjobserver/config: jobserver
```

## Customize
Apart from the above mentioned build arguments and environment variables the docker image can be extended as desired by using the docker `FROM` instruction.

An important use case for the customization is for example providing a custom built spark, which could be solved e.g. with the following custom `Dockerfile`:
```
FROM jobserver

# Install dependencies
RUN apk add --no-cache bash curl

# Replace spark binary with custom one
# (lying in the build context locally)
COPY spark.tgz /tmp/spark.tgz
RUN mkdir -p /tmp/spark && \
    tar -xvzf /tmp/spark.tgz -C /tmp/spark --strip-components 1 && \
    mv /tmp/spark /opt/spark

# Run jobserver
CMD /opt/spark/bin/spark-submit \
    --class spark.jobserver.JobServer \
    --driver-memory "${SPARK_JOBSERVER_MEMORY}" \
    --conf "spark.executor.extraJavaOptions=${LOGGING_OPTS}" \
    --driver-java-options "${LOGGING_OPTS}" \
    /opt/sparkjobserver/bin/spark-job-server.jar \
    /opt/sparkjobserver/config/jobserver.conf
```