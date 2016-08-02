package spark.jobserver

import java.io.File
import java.lang.Float
import java.net.URL

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.cache.Cache
import spark.jobserver.io.JobDAOActor
import spark.jobserver.util.{ContextURLClassLoader, JarUtils}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class JobCacheImpl(cacheConfig: Config,
                   dao: ActorRef,
                   sparkContext: SparkContext,
                   loader: ContextURLClassLoader) extends JobCache[api.SparkJobBase] {

  type CacheType = Cache[String, JobJarInfo[api.SparkJobBase]]

  implicit private val daoAskTimeout: Timeout = Timeout(3 seconds)

  private val cacheDriver = Try(cacheConfig.getString("driver")).getOrElse("spark.jobserver.cache.LRUCache")
  private val maxEntries = Try(cacheConfig.getInt("max-entries")).getOrElse(10000)
  private val loadingFactor = Try(cacheConfig.getDouble("load-factor")).getOrElse(0.75).toFloat
  private val cacheEnabled = Try(cacheConfig.getBoolean("enabled")).getOrElse(true)
  private val logger = LoggerFactory.getLogger(getClass)
  private val cache = JarUtils.loadClassWithArgs[CacheType](
    cacheDriver,
    Seq(Integer.valueOf(maxEntries), Float.valueOf(loadingFactor))
  )

  def getJobViaDao(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo[api.SparkJobBase] = {
    val jarPathReq = (dao ? JobDAOActor.GetJarPath(appName, uploadTime)).mapTo[JobDAOActor.JarPath]
    val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).jarPath
    val jarFilePath = new File(jarPath).getAbsolutePath
    sparkContext.addJar(jarFilePath)
    loader.addURL(new URL("file:" + jarFilePath))
    val constructor = Try(JarUtils.loadClassOrObject[api.SparkJobBase](classPath, loader)).get
    JobJarInfo[api.SparkJobBase](() => constructor, classPath, jarFilePath)
  }

  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo[api.SparkJobBase] = {
    logger.info(s"Loading app: $appName at $uploadTime")
    if (cacheEnabled) {
      cache.getOrPut((appName, uploadTime, classPath).toString, getJobViaDao(appName, uploadTime, classPath))
    }else{
      getJobViaDao(appName, uploadTime, classPath)
    }
  }
}