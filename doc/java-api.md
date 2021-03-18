## SJS Java Api

The Java API in the Spark Job Server is designed to be a more Java friendly API for users to run Spark Jobs
inside of SJS.

There are a few differences between this API and the standard one.

- The API is entirely Java based
- Type safety is done via generics instead of type aliases
- Requires it's own set of Context factories specific to Java (very important)

### Basic Example

```java
public class JavaConfigurableJob implements JavaSparkJob<String, String> {

    public String run(JavaSparkContext sc, JobEnvironment runtime, String data) {
        return data;
    }

    public String verify(JavaSparkContext sc, JobEnvironment runtime, Config config) throws RuntimeException {
        String input = config.getString("input");
        if (input.length() == 0) {
            throw new IllegalArgumentException("You passed an empty string");
        }
        return input;
    }
}
```
[Full File Here](https://github.com/spark-jobserver/spark-jobserver/blob/master/job-server-tests/src/main/java/spark/jobserver/JavaConfigurableJob.java)

This example does nothing Spark based, but gives an idea of the difference in structure. In order to run a job
written like this, the user **MUST** use the `JavaSparkContextFactory`. This context is configured the same as
any of the Context factories in the Scala API. In addition, there are context types for all Java related jobs.
In the Java API, there are both context factories and interfaces that must be used for each kind of job.
For instance the JSparkJob uses the JavaSparkContextFactory.

###Additional Context and Job Types

- `JavaSqlContextFactory` for SQLContext Jobs with `JavaSqlJob`
- `JavaStreamingContextFactory` for StreamingContext jobs with `JavaStreamingJob`

**NOTE: If you do not use the correct context, the jobs will probably not run correctly, it is imperative you configure
jobs correctly.**


### Convenience Job Interface
Besides the `JavaSparkJob<D, R>`, which configures the return value of `verify(...)` and input to `run(...)` as well as the
return value of `run(...)`, SJS offers the `JSparkJob<R>` which defaults to `JavaSparkJob<Config, R>`. You can use this
interface if you want to pass the spark configuration to `run(...)`.

```java
public class JavaHelloWorldJob implements JSparkJob<String> {

    public String run(JavaSparkContext sc, JobEnvironment runtime, Config data) {
        return "Hi!";
    }

    public Config verify(JavaSparkContext sc, JobEnvironment runtime, Config config) throws RuntimeException{
        return ConfigFactory.empty();
    }
}
```
[Full File Here](https://github.com/spark-jobserver/spark-jobserver/blob/master/job-server-tests/src/main/java/spark/jobserver/JavaHelloWorldJob.java)

Similarly to `JavaSparkJob`, additional base classes exist for SQLContext Jobs (`JSqlJob`) and StreamingContext jobs (`JStreamingJob`).