##SJS Spark Api

The Spark API in the Spark Job Server is designed for users to run Spark Jobs inside of SJS.

###Basic Example

```scala
object WordCountExampleNewApi extends NewSparkJob {
  type JobData = Seq[String]
  type JobOutput = collection.Map[String, Long]

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
    sc.parallelize(data).countByValue

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
    JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
```
[Full File Here](https://github.com/spark-jobserver/spark-jobserver/blob/master/job-server-tests/src/main/scala/spark/jobserver/WordCountExample.scala)

This job counts word in a given input string. `JobData` defines the input data created by `validate` and passed to
`runJob`. `JobOutput` is returned by `runJob`.

`validate` is supposed the parse the passed `Config` to a valid input. If the input is invalid, `validate` is supposed
to return a `ValidationProblem`.
