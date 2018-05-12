<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Logging Details and Configurations](#logging-details-and-configurations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Logging Details and Configurations
###Client Mode
The Jobserver creates 1 application log file in `LOG_DIR` and 3 more log files for each context in `${LOG_DIR}/jobserver-<context name + hash>/`. Example:  
`/var/log/job-server/spark-job-server.log` <== Application log  
`/var/log/job-server/jobserver-context-name81236192412312231124/spark-job-server.log` <== Driver log  
`/var/log/job-server/jobserver-context-name81236192412312231124/spark-job-server.out` <== Job output  
`/var/log/job-server/jobserver-context-name81236192412312231124/gc.out` <== Garbage collection output

`LOG_DIR` is configured in \<environment>.sh). Example:  
`LOG_DIR=/var/log/job-server`    

If `LOG_DIR` is not set, the Jobserver will use a temporary directory: `/tmp/job-server`.
###Cluster Mode
Everything is the same in Cluster Mode, however the filepath and name of the Application log file is set like this:

`MANAGER_LOGGING_OPTS` is configured in \<environment>.sh. Example:  
`MANAGER_LOGGING_OPTS="-Dlog4j.configuration=log4j-server.properties"`    

In `log4j-server.properties`, `log4j.appender.LOGFILE.File` sets the filepath and name for the Application log. Example:  
`log4j.appender.LOGFILE.File=${LOG_DIR}/spark-job-server.log`