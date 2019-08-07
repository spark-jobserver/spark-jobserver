# Jobserver Integration tests
This project contains a set of integration tests that can be run against a running deployment of jobserver.
This way it can be verified that the whole stack (including configuration, spark, dao, network) is working as intended.

## Usage
Integration tests can be run locally with a regular test runner or by invoking the provided main class `IntegrationTest`.
To run the tests against an arbitrary jobserver from a remote environment you can:
```shell
# Assemble a fat jar
sbt assembly

# Move the fat jar to a remote location
scp target/scala-*/job-server-integration-tests-assembly-*.jar <remote-path>/integration-tests.jar

# Invoke the test at the remote location
java -jar integration-tests.jar                                     # displays usage
java -jar integration-tests.jar <jobserver-ip>:<jobserver-port>     # executes tests for a specific jobserver

```
