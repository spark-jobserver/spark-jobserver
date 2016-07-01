package spark.jobserver

import java.net.URL

import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.JobDAOActor
import spark.jobserver.util.{ContextURLClassLoader, JarUtils, LRUCache}
import spark.jobserver.api._

case class JobJarInfo(constructor: () => spark.jobserver.api.SparkJobBase,
                      className: String,
                      jarFilePath: String)

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the jar and load it every single time?
 */

class JobCacheImpl(maxEntries: Int,
                   dao: ActorRef,
                   sparkContext: SparkContext,
                   loader: ContextURLClassLoader) extends JobCache {
  import scala.concurrent.duration._

  private val cache = new LRUCache[(String, DateTime, String), JobJarInfo](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(3 seconds)

  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the jar was uploaded
   * @param uploadTime the upload time for the version of the jar wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    cache.get((appName, uploadTime, classPath), {
      import akka.pattern.ask

      import scala.concurrent.Await

      val jarPathReq = (dao ? JobDAOActor.GetJarPath(appName, uploadTime)).mapTo[JobDAOActor.JarPath]
      val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).jarPath
      val jarFilePath = new java.io.File(jarPath).getAbsolutePath()
      sparkContext.addJar(jarFilePath) // Adds jar for remote executors
      loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
      val constructor = JarUtils.loadClassOrObject[spark.jobserver.api.SparkJobBase](classPath, loader)
      JobJarInfo(constructor, classPath, jarFilePath)
    })
  }
}
