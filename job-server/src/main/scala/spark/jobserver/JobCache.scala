package spark.jobserver

import java.net.URL

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.cache.LRUCache
import spark.jobserver.context.JavaToScalaWrapper
import spark.jobserver.io.JobDAOActor
import spark.jobserver.util.{ContextURLClassLoader, JarUtils}

case class JobJarInfo(constructor: () => api.SparkJobBase,
                      className: String,
                      jarFilePath: String)

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the jar and load it every single time?
 */

class JobCacheImpl(maxEntries: Int,
                   cacheDriver: String,
                   dao: ActorRef,
                   sparkContext: SparkContext,
                   loader: ContextURLClassLoader) extends JobCache {

  private val cache = new LRUCache[String, JobJarInfo](maxEntries)

  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(3 seconds)

  implicit def JavaJob2Scala(j: JavaSparkJob[_, _, _]): api.SparkJobBase = new JavaToScalaWrapper(j)

  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the jar was uploaded
   * @param uploadTime the upload time for the version of the jar wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    cache.getOrPut((appName, uploadTime, classPath).toString(), {
      val jarPathReq = (dao ? JobDAOActor.GetJarPath(appName, uploadTime)).mapTo[JobDAOActor.JarPath]
      val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).jarPath
      val jarFilePath = new java.io.File(jarPath).getAbsolutePath
      sparkContext.addJar(jarFilePath) // Adds jar for remote executors
      loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
        val constructor = JarUtils.loadClassOrObject[api.SparkJobBase](classPath, loader)
        JobJarInfo(() => constructor, classPath, jarFilePath)
    })
  }
}
