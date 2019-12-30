<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CombinedDAO](#combineddao)
  - [Configuration](#configuration)
    - [HDFS + Zookeeper](#hdfs--zookeeper)
      - [Autopurge feature](#autopurge-feature)
    - [HDFS + H2](#hdfs--h2)
    - [H2](#h2)
    - [PostgreSQL](#postgresql)

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

#### HDFS + H2
Please configure H2 backend as described [here](../README.md#configuring-spark-jobserver-backend)

To set up the CombinedDAO with HDFS and H2, include the following lines in the configuration file (e.g. `local.conf`):

```
    spark {
        jobserver {
            jobdao = spark.jobserver.io.CombinedDAO
            combineddao {
              rootdir = "/tmp/combineddao"
              binarydao {
                class = spark.jobserver.io.HdfsBinaryDAO
              }
              metadatadao {
                    class = spark.jobserver.io.MetaDataSqlDAO
                  }
            }
            sqldao {
                  # Slick database driver, full classpath
                  slick-driver = slick.driver.H2Driver

                  # JDBC driver, full classpath
                  jdbc-driver = org.h2.Driver

                  # Directory where default H2 driver stores its data. Only needed for H2.
                  rootdir = "/tmp/spark-jobserver/metasqldao/data"

                  jdbc {
                    url = "jdbc:h2:file:/tmp/spark-jobserver/metasqldao/data/h2-db"
                    user = "secret"
                    password = "secret"
                  }

                  dbcp {
                    maxactive = 20
                    maxidle = 10
                    initialsize = 10
                  }
             }
        }

    flyway.locations="db/combineddao/h2/migration"
```

#### H2
Please configure H2 backend as described [here](../README.md#configuring-spark-jobserver-backend)

To set up the CombinedDAO only with H2 backend, include the following lines in the configuration file (e.g. `local.conf`):

```
    spark {
        jobserver {
            jobdao = spark.jobserver.io.CombinedDAO
            combineddao {
              rootdir = "/tmp/combineddao"
              binarydao {
                class = spark.jobserver.io.BinarySqlDAO
              }
              metadatadao {
                    class = spark.jobserver.io.MetaDataSqlDAO
                  }
            }
            sqldao {
                  # Slick database driver, full classpath
                  slick-driver = slick.driver.H2Driver

                  # JDBC driver, full classpath
                  jdbc-driver = org.h2.Driver

                  # Directory where default H2 driver stores its data. Only needed for H2.
                  rootdir = "/tmp/spark-jobserver/metasqldao/data"

                  jdbc {
                    url = "jdbc:h2:file:/tmp/spark-jobserver/metasqldao/data/h2-db"
                    user = "secret"
                    password = "secret"
                  }

                  dbcp {
                    maxactive = 20
                    maxidle = 10
                    initialsize = 10
                  }
             }
        }
    }

    flyway.locations="db/combineddao/h2/migration"
```

#### PostgreSQL

Ensure that you have spark_jobserver database created with necessary rights
granted to user.

    # create database user jobserver and database spark_jobserver:
    $ createuser --username=<superuser> -RDIElPS jobserver
    $ createdb -Ojobserver -Eutf8 spark_jobserver
    CTRL-D -> logout from psql

    # logon as superuser and enable the large object extension:
    $ psql -U <superuser> spark_jobserver
    spark_jobserver=# CREATE EXTENSION lo;
    CTRL-D -> logout from psql

    # you can connect to the database using the psql command line client:
    $ psql -U jobserver spark_jobserver

To set up the CombinedDAO only with PostgreSQL backend, include the following lines in the configuration file (e.g. `local.conf`):

```
    spark {
        jobserver {
            jobdao = spark.jobserver.io.CombinedDAO
            combineddao {
              rootdir = "/tmp/combineddao"
              binarydao {
                    class = spark.jobserver.io.BinarySqlDAO
                  }
              metadatadao {
                    class = spark.jobserver.io.MetaDataSqlDAO
                  }
            }
            sqldao {
                  # Slick database driver, full classpath
                  slick-driver = slick.driver.PostgresDriver

                  # JDBC driver, full classpath
                  jdbc-driver = org.postgresql.Driver

                  jdbc {
                    url = "jdbc:postgresql://db_host/spark_jobserver"
                    user = "jobserver"
                    password = "secret"
                  }

                  dbcp {
                    maxactive = 20
                    maxidle = 10
                    initialsize = 10
                  }
             }
        }
    }

    flyway.locations="db/combineddao/postgresql/migration"
```
