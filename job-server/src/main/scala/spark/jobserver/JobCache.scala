package spark.jobserver

import java.io.{File, IOException}
import java.net.URL
import java.nio.file.{Files, Paths}

import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.{BinaryType, JobDAOActor}
import spark.jobserver.japi.BaseJavaJob
import spark.jobserver.util.{ContextURLClassLoader, JarUtils, LRUCache}

import akka.pattern.ask

import scala.concurrent.Await

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the binary and load it every single time?
 */

class JobCacheImpl(maxEntries: Int,
                   dao: ActorRef,
                   sparkContext: SparkContext,
                   loader: ContextURLClassLoader) extends JobCache {
  import scala.concurrent.duration._

  private val cache = new LRUCache[(String, DateTime, String, BinaryType), BinaryJobInfo](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(60 seconds)

  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the binary was uploaded
   * @param uploadTime the upload time for the version of the binary wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    cache.get((appName, uploadTime, classPath, BinaryType.Jar), {
      logger.info("Begin to get jar path for app {}, uploadTime {} from dao {}", appName,
        uploadTime, dao.path.toSerializationFormat)
      val jarPathReq =
        (dao ? JobDAOActor.GetBinaryPath(appName, BinaryType.Jar, uploadTime)).mapTo[JobDAOActor.BinaryPath]
      val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).binPath
      logger.info("End of get jar path for app {}, uploadTime {}, jarPath {}", appName, uploadTime, jarPath)
      val jarFile = Paths.get(jarPath)
      if (!Files.exists(jarFile)) {
        logger.info("Local jar path {} not exist, fetch binary content from remote actor", jarPath)
        val jarBinaryReq = (dao ? JobDAOActor.GetBinaryContent(appName, BinaryType.Jar, uploadTime))
          .mapTo[JobDAOActor.BinaryContent]
        val binaryJar = Await.result(jarBinaryReq, daoAskTimeout.duration)
        logger.info("Writing {} bytes to file {}", binaryJar.content.size, jarFile.toAbsolutePath.toString)
        try {
          if (!Files.exists(jarFile.getParent)) {
            logger.info("Creating cache dir {}", jarFile.getParent.toAbsolutePath.toString)
            Files.createDirectories(jarFile.getParent)
          }
          Files.write(jarFile, binaryJar.content)
        } catch {
          case e: IOException => logger.error("Write to path {} error {}", jarPath: Any, e)
        }
      }
      val jarFilePath = jarFile.toAbsolutePath.toString
      sparkContext.addJar(jarFilePath) // Adds jar for remote executors
      loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
      val constructor = JarUtils.loadClassOrObject[spark.jobserver.api.SparkJobBase](classPath, loader)
      JobJarInfo(constructor, classPath, jarFilePath)
    }).asInstanceOf[JobJarInfo]
  }

  def getJavaJob(appName: String, uploadTime: DateTime, classPath: String): JavaJarInfo = {
    cache.get((appName, uploadTime, classPath, BinaryType.Jar), {
      val jarPathReq =
        (dao ? JobDAOActor.GetBinaryPath(appName, BinaryType.Jar, uploadTime)).mapTo[JobDAOActor.BinaryPath]
      val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).binPath
      val jarFilePath = new File(jarPath).getAbsolutePath
      sparkContext.addJar(jarFilePath) // Adds jar for remote executors
      loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
      val constructor = JarUtils.loadClassOrObject[BaseJavaJob[_, _]](classPath, loader)
      JavaJarInfo(constructor.apply(), classPath, jarFilePath)
    }).asInstanceOf[JavaJarInfo]
  }

  /**
    * Retrieves a Python job egg location from the cache if it's there, otherwise use the DAO to retrieve it.
    * @param appName the appName under which the binary was uploaded
    * @param uploadTime the upload time for the version of the binary wanted
    * @param classPath the fully qualified name of the class/object to load
    * @return The case class containing the location of the binary file for the specified job.
    */
  override def getPythonJob(appName: String, uploadTime: DateTime, classPath: String): PythonJobInfo = {
    cache.get((appName, uploadTime, classPath, BinaryType.Egg), {
      val pyPathReq =
        (dao ? JobDAOActor.GetBinaryPath(appName, BinaryType.Egg, uploadTime)).mapTo[JobDAOActor.BinaryPath]
      val pyPath = Await.result(pyPathReq, daoAskTimeout.duration).binPath
      val pyFilePath = new File(pyPath).getAbsolutePath
      PythonJobInfo(pyFilePath)
    }).asInstanceOf[PythonJobInfo]
  }
}
