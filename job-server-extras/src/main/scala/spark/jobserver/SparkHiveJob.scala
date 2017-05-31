package spark.jobserver

import org.apache.spark.sql.hive.HiveContext

trait SparkHiveJob extends spark.jobserver.api.SparkJobBase {
  type C = HiveContext
}
