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
java -jar integration-tests.jar </path/to/config/file>              # executes tests on a specific deployment

```

A configuration of the integration tests is possible by supplying a config file as parameter.
Within the config file you can specify:
* Address(es) of the jobserver deployment(s) to be tested
* Names of the test to be run
* Which deployment controller to take for HA tests (i.e. how to stop and restart jobservers for testing)
* Possibly additional fields required for tests or deployment controller

Here's a running example for a config file:
```javascript
{
  // Define the addresses of to be tested jobservers in this format
  jobserverAddresses: ["localhost:8090", "localhost:8091"]
  // Specify which tests to run (list of concrete tests)
  runTests: ["BasicApiTests", "CornerCasesTests", "HAFailoverTests"]
  // Define the concrete deployment controller in case you are running HA tests
  deploymentController: "BoshController"
  // Any other optional fields
  deployment: "spark-cluster-test"
}
```
