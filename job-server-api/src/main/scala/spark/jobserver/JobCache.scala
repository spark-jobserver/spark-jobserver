package spark.jobserver

import org.joda.time.DateTime

case class JobJarInfo(constructor: () => api.SparkJobBase,
                      className: String,
                      jarFilePath: String)

trait JobCache {
  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the jar was uploaded
   * @param uploadTime the upload time for the version of the jar wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo
}