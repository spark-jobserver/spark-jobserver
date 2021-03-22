<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Metadata and Binary DAOs](#metadata-and-binary-daos)
  - [Configuration](#configuration)
    - [HDFS + Zookeeper](#hdfs--zookeeper)
      - [Autopurge feature](#autopurge-feature)
    - [HDFS + H2](#hdfs--h2)
    - [H2](#h2)
    - [PostgreSQL](#postgresql)
    - [MySQL](#mysql)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Metadata and Binary DAOs

Storing BLOBs in a database is considered bad practice and Jobserver therefore
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
- SqlBinaryDAO (H2, PostgreSQL, MySQL)

JobDAOActor acts as a glue between MetaDataDAO and BinaryDAO. The foreign key between
MetaDataDAO and BinaryDAO is the hash of binary.


### Configuration

#### HDFS + Zookeeper
To set up HDFS binary and Zookeeper metadata DAO, include the following lines in the configuration file (e.g. `local.conf`):
```
    daorootdir = "/tmp/spark-jobserver"
    binarydao {
        class = spark.jobserver.io.HdfsBinaryDAO
        dir = "hdfs://<hadoop-ip>:<hadoop-port>/spark-jobserver/binaries"
    }
    metadatadao {
        class = spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
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

To set up HDFS binary and H2 metadata DAO, include the following lines in the configuration file (e.g. `local.conf`):

```
    spark {
        jobserver {
            daorootdir = "/tmp/spark-jobserver"
            binarydao {
                class = spark.jobserver.io.HdfsBinaryDAO
            }
            metadatadao {
                class = spark.jobserver.io.MetaDataSqlDAO
            }
            sqldao {
                  # Slick database driver, full classpath
                  slick-driver = slick.jdbc.H2Profile

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

    flyway.locations="db/h2/migration"
```

#### H2
Please configure H2 backend as described [here](../README.md#configuring-spark-jobserver-backend)

To set up the DAO only with H2 backend, include the following lines in the configuration file (e.g. `local.conf`):

```
    spark {
        jobserver {
            daorootdir = "/tmp/spark-jobserver"
            binarydao {
                class = spark.jobserver.io.BinarySqlDAO
            }
            metadatadao {
                class = spark.jobserver.io.MetaDataSqlDAO
            }
            sqldao {
                  # Slick database driver, full classpath
                  slick-driver = slick.jdbc.H2Profile

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

    flyway.locations="db/h2/migration"
```

Be sure to add [AUTO_MIXED_MODE](http://h2database.com/html/features.html#auto_mixed_mode) to your
H2 JDBC URL; this allows multiple processes to share the same H2 database using a lock file.

In yarn-client mode, use H2 in server mode as described below instead of embedded mode.
- Download the full H2 jar from http://www.h2database.com/html/download.html and follow docs.
- Note that the version of H2 should match the H2 client version bundled with spark-jobserver, currently 1.3.176.


A sample JDBC configuration is below:
```
jdbc {
        url = "jdbc:h2:tcp://localhost//ROOT/PARENT/DIRECTORIES/spark_jobserver"
        user = "secret"
        password = "secret"
      }

```
Note: /ROOT/PARENT/DIRECTORIES/spark_jobserver is the absolute path to a directory to which H2 has write access.


Example command line to launch H2 Server:
```
java -cp h2-1.3.176.jar org.h2.tools.Server -tcp
```
Use -? on command line to see other options.


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

To set up the DAO only with PostgreSQL backend, include the following lines in the configuration file (e.g. `local.conf`):

```
    spark {
        jobserver {
            daorootdir = "/tmp/spark-jobserver"
            binarydao {
                class = spark.jobserver.io.BinarySqlDAO
            }
            metadatadao {
                class = spark.jobserver.io.MetaDataSqlDAO
            }
            sqldao {
                  # Slick database driver, full classpath
                  slick-driver = slick.jdbc.PostgresProfile

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

    flyway.locations="db/postgresql/migration"
```

#### MySQL

Ensure that you have spark_jobserver database created with necessary rights
granted to user.

    # secure your mysql installation and define password for mysql root user
    $ mysql_secure_installation

    # logon as database root
    $ mysql -u root -p

    # create a database user and a database for spark jobserver:
    mysql> CREATE USER 'jobserver'@'localhost' IDENTIFIED BY 'secret';
    mysql> CREATE DATABASE spark_jobserver;
    mysql> GRANT ALL ON spark_jobserver.* TO 'jobserver'@'localhost';
    mysql> FLUSH PRIVILEGES;
    CTRL-D -> logout from mysql

    # you can connect to the database using the mysql command line client:
    $ mysql -u jobserver -p

To use MySQL as backend include the following lines in the configuration file (e.g. `local.conf`).
```
    spark {
      jobserver {
        daorootdir = "/tmp/spark-jobserver"
        binarydao {
            class = spark.jobserver.io.BinarySqlDAO
        }
        metadatadao {
            class = spark.jobserver.io.MetaDataSqlDAO
        }
        sqldao {
          # Slick database driver, full classpath
          slick-driver = slick.jdbc.MySQLProfile

          # JDBC driver, full classpath
          jdbc-driver = com.mysql.jdbc.Driver

          jdbc {
            url = "jdbc:mysql://db_host/spark_jobserver"
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

    # also add the following line at the root level.
    flyway.locations="db/mysql/migration"
```