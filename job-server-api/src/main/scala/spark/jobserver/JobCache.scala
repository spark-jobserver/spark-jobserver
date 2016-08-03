package spark.jobserver

import org.joda.time.DateTime
import spark.jobserver.api.JSparkJob

trait SparkJobInfo
case class JobJarInfo(constructor: () => api.SparkJobBase,
                      className: String,
                      jarFilePath: String) extends SparkJobInfo

case class JavaJarInfo(constructor: () => JSparkJob[_],
                       className: String,
                       jarFilePath: String) extends SparkJobInfo {
  def job(): JSparkJob[_] = constructor.apply()
}

trait JobCache {
  @throws[ClassNotFoundException]
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo
  @throws[ClassNotFoundException]
  def getJavaJob(appName: String, uploadTime: DateTime, classPath: String): JavaJarInfo
}