package spark.jobserver

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.api.java.JavaStreamingContext
import spark.jobserver.japi.BaseJavaJob

trait JSqlJob[R] extends BaseJavaJob[R, SQLContext]

trait JHiveJob[R] extends BaseJavaJob[R, HiveContext]

trait JStreamingJob[R] extends BaseJavaJob[R, JavaStreamingContext]