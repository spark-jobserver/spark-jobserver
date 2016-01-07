package spark.jobserver

import org.apache.spark.sql.DataFrame
case class NamedDataFrame(df: DataFrame) extends NamedObject

