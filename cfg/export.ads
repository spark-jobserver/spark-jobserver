artifacts builderVersion: "1.1", {
  group "spark.jobserver", {
    artifact "sparkjobserver", {
      file "$componentroot/job-server/target/scala-2.10/spark-job-server.jar"
    }
  }
}
