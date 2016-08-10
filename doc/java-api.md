## The Java Api in Spark Job Server 0.7.0

#### Entrance Points
In the new java api, the job that the user implements depends upon what kind of job context
they wish to use in their job. The following interfaces exist with their corresponding
context type.


| Job Class | Context Type |
|:---:|:---:|
| `JSparkContextJob` | Java Spark Context |
| `JSparkHiveJob` | Hive Context |
| `JSparkSqlJob` | SQL Context |
|`JSparkStreamingJob`| Streaming Context |

These classes function as the central point of entrance for the Java API. In addition,
there are several context factories available that must be used with their corresponding
Context type, this is because there is a line drawn between the Scala and the Java API, so
they both have their respective implementation of context factories.

| Context Type | Context Factory |
| :---: | :---: |
| Java Spark Context | `JavaContextFactory` |
| SQL Context | `JavaSqlContextFactory` |
| Streaming Context | `JavaStreamingContextFactory` |

Please note that for the `JavaStreamingContextFactory` this class provides a 
`org.apache.spark.streaming.StreamingContext` instead of it's Java equivalent. This is because
this in order for the factory to be compatible with our interfaces it had to return an underlying
`SparkContext`

#### Example Job

Several example Jobs are found in the `spark.jobserver.examples` package

```java
public class JavaJobTest extends JSparkContextJob<Integer> {

    @Override
    public Integer runJob(JavaSparkContext context, JobEnvironment jEnv, Config data) {
        return 0;
    }

    @Override
    public JobValidation validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        return new JobValidation.JOB_VALID();
    }
}
```

Above is a simple example that does, well, nothing. But one can see the basics of the interface from this example and
using this we can explain the parts of the job.

##### The Validate Step

The validate method is just like what it is in the Scala API, except instead of using `Good` or `Bad` from Scalatic there are
basic static beans that are used to determine whether or not the job is valid or not. These 2 validation statuses are the
`JobValidation.JOB_VALID(Config cfg)` and the `JobValidation.JOB_INVALID(Exception e)` classes, each of which hold information
regarding what had happened with the validation. If no configuration is provided to a `JobValidation.JOB_VALID()` then it will
use a default constructor with `ConfigFactory.empty()` for it's job configuration. The validate method is passed into it a Spark
Context of the type desired by the user, a job environment and a config. Note that unlike the Scala API, the job data for Java
jobs is done entirely through the TypeSafe Config library. So users must build a proper valid config to pass to their runJob or
build an exception containing why the job failed in this method.

##### The Run Job Step

The runJob method is also exactly like what it is in the Scala API, except instead of the JobData type a TypeSafe Config is passed
in. So the user must extract their data from that object. The user is provided with a Spark Context that they can then use to do
their data manipulations. Then the return type is the parametrized type in the job extension. The Java API is able to serialize and return
all the relevant types the Scala API can, but returns the Java equivalents. 

#### A Full Example

```java
public class WordCountJava extends JSparkContextJob<Map<String, Long>> {

    @Override
    public Map<String, Long> runJob(JavaSparkContext context, JobEnvironment jEnv, Config data) {
        return context.parallelize(data.getStringList("input")).countByValue();
    }

    @Override
    public JobValidation validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        try {
            final List<String> input = Arrays.asList(cfg.getString("input").split(" "));
            return new JobValidation.JOB_VALID(ConfigFactory.parseString("input = " + input.toString()));
        } catch (Exception e) {
            return new JobValidation.JOB_INVALID(e);
        }
    }

}
```

The results of the job are something like the following when ran with the following commands locally

First a context of the proper type is created...
`curl -d "" 'localhost:8090/contexts/test-context2?context-factory=spark.jobserver.context.JavaContextFactory&num-cpu-cores=4&memory-per-node=2g'`

Then the job is ran using that context...
`curl -d "input = java java scala python java haskell ruby" 'localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountJava&context=test-context2&sync=true'`

```json
{
  "duration": "0.04 secs",
  "classPath": "spark.jobserver.WordCountJava",
  "startTime": "2016-08-07T22:59:03.371-04:00",
  "context": "test-context2",
  "result": "{haskell=1, java=3, scala=1, python=1, ruby=1}",
  "status": "FINISHED",
  "jobId": "5edee92c-7922-4e3f-9958-2422d50bb727"
}
```

More examples can be found in the job-server-tests project and under `spark.jobserver.examples`