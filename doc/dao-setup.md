<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CombinedDAO](#combineddao)
  - [Configuration](#configuration)
    - [HDFS + Zookeeper](#hdfs--zookeeper)
    - [HDFS + SQL](#hdfs--sql)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## CombinedDAO

Storing BLOBs in a database is considered bad practice and CombinedDAO therefore
provides an opportunity to use combined backend and store BLOBs (binaries) separately
in a database, which is designed for it.
Initial proposal and discussion can be found [here](https://github.com/spark-jobserver/spark-jobserver/issues/1148).

MetaDataDAO represents all the metadata within Jobserver including the metadata of binaries.
Current MetaDataDAO implementations:
- MetaDataSqlDAO (H2, PostgreSQL, MySQL)
- MetaDataZookeeperDAO (Zookeeper DB)

BinaryDAO represents a simple interface for CRUD operations related to storing BLOBs.
Current BinaryDAO implementations:
- HDFSBinaryDAO

CombinedDAO acts as a glue between MetaDataDAO and BinaryDAO. The foreign key between
MetaDataDAO and BinaryDAO is the hash of binary.


### Configuration

#### HDFS + Zookeeper
To set up the CombinedDAO with HDFS and Zookeeper, include the following lines in the configuration file (e.g. `local.conf`):
```
    jobdao = spark.jobserver.io.CombinedDAO
    combineddao {
      rootdir = "/tmp/combineddao"
      binarydao {
        class = spark.jobserver.io.HdfsBinaryDAO
        dir = "hdfs://<hadoop-ip>:<hadoop-port>/spark-jobserver/binaries"
      }
      metadatadao {
        class = spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
      }
    }

    zookeeperdao {
      dir = "jobserver/db"
      connection-string = "localhost:2181"
      autopurge = true
      autopurge_after_hours = 168
    }
```

HdfsBinaryDAO settings:

`dir` - directory to store data in. If local path (e.g. `/tmp/binarydao`) is used, then the
local file system will be used instead of HDFS.

Zookeeperdao settings:

`dir` - directory to store data in. If left empty then the root directory (`/`) of
Zookeeper will be used.

`connection-string` -  comma separated `host:port` pairs, each corresponding to a Zookeeper server
e.g. "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183,127.0.0.1:2184".

`autopurge` -  A boolean specifying if the autopurge feature of Zookeeper DAO should be used or not.
If no such property is specified, it defaults to `false`.

`autopurge_after_hours` -  Integer value specifying the age of contexts and jobs in **hours**, after which they are considered old and purged in the next run.
If no such property is specified, it defaults to `168` hours (7 days).

##### Autopurge feature
Due to the hierarchical nature of our Zookeeper schema, read queries require to iterate over all nodes and process each of them.
Especially within a distributed setup large amounts of Zookeeper nodes can therefore reduce the Jobserver performance.
The autopurge feature introduces an option to automatically remove old contexts and jobs to contain this effect.

It works by starting an `AutoPurgeActor` on Jobserver startup, which every hour checks the zookeeper tree for jobs and contexts which have been finished for more than `autopurge_age` hours and deletes them.

#### HDFS + SQL
Please configure SQL backend as described [here](../README.md#configuring-spark-jobserver-backend)

To set up the CombinedDAO with HDFS and SQL, include the following lines in the configuration file (e.g. `local.conf`):

```
    jobdao = spark.jobserver.io.CombinedDAO
    combineddao {
      binarydao {
        class = spark.jobserver.io.HdfsBinaryDAO
      }
      metadatadao {
        class = spark.jobserver.io.MetaDataSqlDAO
      }
    }
```
