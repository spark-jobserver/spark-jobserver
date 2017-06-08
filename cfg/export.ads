artifacts builderVersion: "1.1", {
  group "com.sap.cp.bigdata.spark", {
    artifact "sparkjobserver", {
      file "$componentroot/job-server-extras/target/scala-2.10/spark-job-server.jar"
    }
  }
}
