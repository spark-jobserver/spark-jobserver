package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, ValidationProblem}

/**
  * A test job that accepts a HiveContext as opposed to the regular SparkContext.
  * Initializes some dummy data into a table, reads it back out, and returns a count.
  * Will create Hive metastore at job-server/metastore_db if Hive isn't configured.
  */
object HiveLoaderJob extends SparkHiveJob {
  // The following data is stored at ./hive_test_job_addresses.txt
  // val addresses = Seq(
  //   Address("Bob", "Charles", "101 A St.", "San Jose"),
  //   Address("Sandy", "Charles", "10200 Ranch Rd.", "Purple City"),
  //   Address("Randy", "Charles", "101 A St.", "San Jose")
  // )

  type JobData = Config
  type JobOutput = Long

  val tableCreate = "CREATE TABLE `default`.`test_addresses`"
  val tableArgs = "(`firstName` String, `lastName` String, `address` String, `city` String)"
  val tableRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
  val tableColFormat = "COLLECTION ITEMS TERMINATED BY '\002'"
  val tableMapFormat = "MAP KEYS TERMINATED BY '\003' STORED"
  val tableAs = "AS TextFile"

  val loadPath = s"'src/main/resources/hive_test_job_addresses.txt'"

  def validate(hive: HiveContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(hive: HiveContext, runtime: JobEnvironment, config: JobData): JobOutput = {
    hive.sql("DROP TABLE if exists `default`.`test_addresses`")
    hive.sql(s"$tableCreate $tableArgs $tableRowFormat $tableColFormat $tableMapFormat $tableAs")

    hive.sql(s"LOAD DATA LOCAL INPATH $loadPath OVERWRITE INTO TABLE `default`.`test_addresses`")
    val addrRdd: DataFrame = hive.sql("SELECT * FROM `default`.`test_addresses`")
    addrRdd.count()
  }
}

/**
  * This job simply runs the Hive SQL in the config.
  */
object HiveTestJob extends SparkHiveJob {
  type JobData = Config
  type JobOutput = Array[Row]

  def validate(hive: HiveContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(hive: HiveContext, runtime: JobEnvironment, config: JobData): JobOutput = {
    hive.sql(config.getString("sql")).collect()
  }
}
