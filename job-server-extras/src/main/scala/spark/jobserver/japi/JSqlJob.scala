package spark.jobserver.japi

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.streaming.StreamingContext

trait JSqlJob[R] extends BaseJavaJob[R, SQLContext]

trait JHiveJob[R] extends BaseJavaJob[R, HiveContext]

trait JTestHiveJob[R] extends BaseJavaJob[R, TestHiveContext]

trait JStreamingJob[R] extends BaseJavaJob[R, StreamingContext]
