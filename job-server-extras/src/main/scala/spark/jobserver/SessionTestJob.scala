package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, ValidationProblem}

/**
  * A test job that accepts a SparkSession.
  * Initializes some dummy data into a table, reads it back out, and returns a count.
  * Will create Hive metastore at job-server/metastore_db if Hive isn't configured.
  */
object SessionLoaderTestJob extends SparkSessionJob {
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
  val tableColFormat = "COLLECTION ITEMS TERMINATED BY '\u0002'"
  val tableMapFormat = "MAP KEYS TERMINATED BY '\u0003' STORED"
  val tableAs = "AS TextFile"

  val loadPath = s"'src/main/resources/hive_test_job_addresses.txt'"

  def validate(spark: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    spark.sql("DROP TABLE if exists `default`.`test_addresses`").foreach(println(_))
    spark.sql(s"$tableCreate $tableArgs $tableRowFormat $tableColFormat $tableMapFormat $tableAs")
      .foreach(println(_))

    spark.sql(s"LOAD DATA LOCAL INPATH $loadPath OVERWRITE INTO TABLE `default`.`test_addresses`")
    val addrRdd: DataFrame = spark.sql("SELECT * FROM `default`.`test_addresses`")
    val count = addrRdd.count()

    count
  }
}

/**
  * This job simply runs the Hive SQL in the config.
  */
object SessionTestJob extends SparkSessionJob {
  type JobData = Config
  type JobOutput = Array[Row]

  def validate(spark: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = Good(config)

  def runJob(spark: SparkSession, runtime: JobEnvironment, config: JobData): JobOutput = {
    val rows = spark.sql(config.getString("sql")).collect()
    spark.sql("DROP TABLE if exists `default`.`test_addresses`").foreach(println(_))
    rows
  }
}
