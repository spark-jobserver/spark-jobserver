#
# Stage 1 : Build
#
FROM openjdk:8-jdk-alpine AS build

# Dependency/environment versions
ARG SBT_VERSION=1.9.7
ARG SCALA_VERSION=2.12.18
ARG SPARK_VERSION=3.4.1
ARG HADOOP_VERSION=3

# Install dependencies
RUN apk add --no-cache bash curl

# Download sbt
RUN curl -sL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" | gunzip | tar -x -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    chmod 0755 /usr/local/bin/sbt

RUN mkdir -p /tmp/spark && \
    mkdir -p /tmp/sparkjobserver

# Install spark
RUN curl -L -o /tmp/spark.tgz http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf /tmp/spark.tgz -C /tmp/spark --strip-components 1

# Build jobserver
COPY . /tmp/sparkjobserver
WORKDIR /tmp/sparkjobserver
RUN echo "Building job server..." && \
    sbt clean job-server-extras/assembly

#
# Stage 2 : Run
#
FROM openjdk:8-jdk-alpine AS jobserver

    # Jobserver settings
ENV SPARK_JOBSERVER_MEMORY=1G \
    LOGGING_OPTS="-Dlog4j.configuration=file:/opt/sparkjobserver/config/log4j.properties" \
    # JobManager settings
    MANAGER_JAR_FILE=/opt/sparkjobserver/bin/spark-job-server.jar \
    MANAGER_CONF_FILE=/opt/sparkjobserver/config/jobserver.conf

RUN apk add --no-cache bash
COPY --from=build /tmp/spark /opt/spark
COPY --from=build /tmp/sparkjobserver/job-server-extras/target/scala-*/spark-job-server.jar /opt/sparkjobserver/bin/spark-job-server.jar

# 8090: API
# 9999: JMX
EXPOSE 8090 9999

# Run jobserver
CMD /opt/spark/bin/spark-submit \
    --class spark.jobserver.JobServer \
    --driver-memory "${SPARK_JOBSERVER_MEMORY}" \
    --conf "spark.executor.extraJavaOptions=${LOGGING_OPTS}" \
    --driver-java-options "${LOGGING_OPTS}" \
    /opt/sparkjobserver/bin/spark-job-server.jar \
    /opt/sparkjobserver/config/jobserver.conf
