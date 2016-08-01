package spark.jobserver

import org.joda.time.DateTime

case class JobJarInfo[+C](constructor: () => C,
                      className: String,
                      jarFilePath: String)

trait JobCache[C] {
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo[C]
  def getJobViaDao(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo[C]
}