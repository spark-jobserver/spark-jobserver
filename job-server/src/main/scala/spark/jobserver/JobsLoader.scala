package spark.jobserver

import java.io.File

import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import spark.jobserver.io.{BinaryType, JobDAOActor}
import spark.jobserver.japi.BaseJavaJob
import spark.jobserver.util.{ContextURLClassLoader, JarUtils, NoSuchBinaryException}
import spark.jobserver.common.akka.metrics.YammerMetrics
import akka.pattern.ask

import scala.concurrent.Await

/**
 * A class to load SparkJob classes.
 */
class JobsLoader(maxEntries: Int,
                 dao: ActorRef,
                 sparkContext: SparkContext,
                 loader: ContextURLClassLoader) extends JobCache with YammerMetrics {
  import scala.concurrent.duration._

  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(60 seconds)

  /**
   * Retrieves the given SparkJob class.
   * @param classPath the sequence of binary paths/names
   * @param mainClass the fully qualified name of the class/object to load
   */
  def getSparkJob(classPath: Seq[String], mainClass: String): JobJarInfo = {
    logger.info("Begin to get jar path for app {}", classPath)
    val constructor = JarUtils.loadClassOrObject[spark.jobserver.api.SparkJobBase](mainClass, loader)
    JobJarInfo(constructor, mainClass, "") // TODO: remove empty string instead of path
  }

  def getJavaJob(classPath: Seq[String], mainClass: String): JavaJarInfo = {
    val constructor = JarUtils.loadClassOrObject[BaseJavaJob[_, _]](mainClass, loader)
    JavaJarInfo(constructor.apply(), mainClass, "")
  }

  /**
    * Retrieves a Python job egg.
    * @param classPath the sequence of binary paths/names
    * @param mainClass the fully qualified name of the class/object to load
    * @return The case class containing the location of the binary file for the specified job.
    */
  override def getPythonJob(classPath: Seq[String], mainClass: String): PythonJobInfo = {
    if (classPath.length == 1) {
      PythonJobInfo(classPath.head)
    } else {
      throw new IllegalArgumentException(
        s"Python should have exactly one egg file! Found: ${classPath.length}")
    }
  }
}
