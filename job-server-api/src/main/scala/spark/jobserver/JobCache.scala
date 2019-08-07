package spark.jobserver

import spark.jobserver.japi.BaseJavaJob

import scala.language.existentials

trait BinaryJobInfo

case class JobJarInfo(constructor: () => api.SparkJobBase,
                      className: String,
                      jarFilePath: String) extends BinaryJobInfo

case class JavaJarInfo(job: BaseJavaJob[_, _],
                       className: String,
                       jarFilePath: String) extends BinaryJobInfo

// For python jobs, there is no class loading or constructor required.
case class PythonJobInfo(eggPath: String) extends BinaryJobInfo

trait JobCache {
  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param classPath the sequence of binary paths/names
   * @param mainClass the fully qualified name of the class/object to load
   */
  def getSparkJob(classPath: Seq[String], mainClass: String): JobJarInfo

  def getJavaJob(classPath: Seq[String], mainClass: String): JavaJarInfo
  /**
    * Retrieves a Python job egg location from the cache if it's there, otherwise use the DAO to retrieve it.
    * @param classPath the sequence of binary paths/names
    * @param mainClass the fully qualified name of the class/object to load
    * @return The case class containing the location of the binary file for the specified job.
    */
  def getPythonJob(classPath: Seq[String], mainClass: String): PythonJobInfo
}
