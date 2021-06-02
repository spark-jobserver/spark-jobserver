# Docker setups
Jobserver setups are vastly different depending on the usage and configuration scenario.
To enable users of spark jobserver with a reference setup and to allow easier integration testing of setup variants, some exemplary setups have been dockerized in this folder.

Contributions of (e.g. your own particular) docker setups are always welcome.

## Usage
Docker setups should come with a **docker-compose** file providing everything you need to start the jobserver including environment.
For example you can run a minimal setup with
```
docker compose -f docker-setups/minimal/docker-compose.yml up
```

## Testing
You can leverage the [job-server-integration-tests](../job-server-integration-tests) to test against a dockerized setup.
An example test configuration is supplied within this folder.
```
sbt clean job-server-integration-tests/assembly
java -jar job-server-integration-tests/target/scala-*/job-server-integration-tests-assembly-*.jar docker-setups/integration-test.conf
```

## Available dockerized setups
The following setups are available:
* [Minimal](minimal)
  Spark Standalone, scala 2.11, spark 2.4.6, hadoop 2.7, single Jobserver, SQL (H2 local) Metadata and Binary Dao, Client mode
* [PostgreSQL](postgres):
  Spark Standalone, scala 2.11, spark 2.4.6, hadoop 2.7, single Jobserver, SQL (Postgresql) Metadata and Binary Dao, Client mode


