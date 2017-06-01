artifacts builderVersion: "1.1", {
  group "com.sap.sparkjobserver", {
    artifact "sparkjobserver-jar", {
      file "$componentroot/job-server/target/scala-2.10/spark-job-server.jar"
    }
  }
}
