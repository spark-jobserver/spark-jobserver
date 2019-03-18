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

Set `HADOOP_CONF_DIR` environment variable for Hadoop and add the following lines to the
configuration file `local.conf`:

```
    combineddao {
      rootdir = "/tmp/combineddao"
      binarydao {
        class = spark.jobserver.io.HdfsBinaryDAO
        dir = "hdfs:///spark-jobserver/binaries"
      }
      metadatadao {
        class = spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
      }
    }

    zookeeperdao {
      dir = "jobserver/db"
      connection-string = "localhost:2181"
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

#### HDFS + SQL

Please configure SQL backend as described [here](../README.md#configuring-spark-jobserver-backend)

Set `HADOOP_CONF_DIR` environment variable for Hadoop and add the following lines to the
configuration file `local.conf`:

```
    combineddao {
      binarydao {
        class = spark.jobserver.io.HdfsBinaryDAO
      }
      metadatadao {
        class = spark.jobserver.io.MetaDataSqlDAO
      }
    }
```
