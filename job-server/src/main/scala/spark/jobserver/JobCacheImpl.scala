package spark.jobserver

import java.io.File
import java.net.URL

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

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

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the jar and load it every single time?
 */

class JobCacheImpl(maxEntries: Int,
                   cacheDriver: String,
                   dao: ActorRef,
                   sparkContext: SparkContext,
                   loader: ContextURLClassLoader) extends JobCache[api.SparkJobBase] {

  private val cache = new LRUCache[String, JobJarInfo[api.SparkJobBase]](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val daoAskTimeout: Timeout = Timeout(3 seconds)

  def getJobViaDao(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo[api.SparkJobBase] = {
    val jarPathReq = (dao ? JobDAOActor.GetJarPath(appName, uploadTime)).mapTo[JobDAOActor.JarPath]
    val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).jarPath
    val jarFilePath = new File(jarPath).getAbsolutePath
    sparkContext.addJar(jarFilePath)
    loader.addURL(new URL("file:" + jarFilePath))
    val constructor = Try(JarUtils.loadClassOrObject[api.SparkJobBase](classPath, loader)).getOrElse(
      new JavaToScalaWrapper(JarUtils.loadClassOrObject[JavaSparkJob[_,_]](classPath, loader))
    )
    JobJarInfo[api.SparkJobBase](() => constructor, classPath, jarFilePath)
  }

  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo[api.SparkJobBase] = {
    logger.info(s"Loading app: $appName at $uploadTime")
    cache.getOrPut((appName, uploadTime, classPath).toString(), getJobViaDao(appName, uploadTime, classPath))
  }
}