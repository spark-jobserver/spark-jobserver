package spark.jobserver

import java.io.File
import java.net.URL

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.api.JSparkJob
import spark.jobserver.cache.Cache
import spark.jobserver.io.JobDAOActor
import spark.jobserver.io.JobDAOActor.JarPath
import spark.jobserver.util.{ContextURLClassLoader, JarUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class JobCacheImpl(cacheConfig: Config, dao: ActorRef, ctx: SparkContext, loader: ContextURLClassLoader)
  extends JobCache {

  type CacheType = Cache[String, SparkJobInfo]

  implicit private val daoAskTimeout: Timeout = Timeout(3 seconds)

  private val cacheDriver = Try(cacheConfig.getString("driver")).getOrElse("spark.jobserver.cache.LRUCache")
  private val cacheEnabled = Try(cacheConfig.getBoolean("enabled")).getOrElse(true)
  private val logger = LoggerFactory.getLogger(getClass)
  private val cache = JarUtils.loadClassWithArgs[CacheType](cacheDriver, cacheConfig)

  private def getJarPath(name: String, uploadTime: DateTime): Future[JarPath] = {
    (dao ? JobDAOActor.GetJarPath(name, uploadTime)).mapTo[JobDAOActor.JarPath]
  }

  private def getJavaViaDao(appName: String, uploadTime: DateTime, classPath: String): Future[JavaJarInfo] = {
    getJarPath(appName, uploadTime)
      .map(j => j.jarPath)
      .map(f => new File(f).getAbsolutePath)
      .map { path =>
        ctx.addJar(path)
        loader.addURL(new URL("file:" + path))
        val constructor = Try(JarUtils.loadClassOrObject[JSparkJob[_, _]](classPath, loader)).get
        JavaJarInfo(() => constructor, classPath, path)
      }
  }

  private def getJobViaDao(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    val jarPathReq = (dao ? JobDAOActor.GetJarPath(appName, uploadTime)).mapTo[JobDAOActor.JarPath]
    val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).jarPath
    val jarFilePath = new File(jarPath).getAbsolutePath
    ctx.addJar(jarFilePath)
    loader.addURL(new URL("file:" + jarFilePath))
    val constructor = Try(JarUtils.loadClassOrObject[api.SparkJobBase](classPath, loader)).get
    JobJarInfo(() => constructor, classPath, jarFilePath)
  }

  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    logger.info(s"Loading app: $appName at $uploadTime")
    if (cacheEnabled) {
      cache.getOrPut(
        (appName, uploadTime, classPath).toString,
        getJobViaDao(appName, uploadTime, classPath)
      ).asInstanceOf[JobJarInfo]
    }else{
      getJobViaDao(appName, uploadTime, classPath)
    }
  }

  def getJavaJob(appName: String, uploadTime: DateTime, classPath: String): JavaJarInfo = {
    logger.info(s"Loading app: $appName at $uploadTime")
    if (cacheEnabled) {
        cache.getOrPut(
          (appName, uploadTime, classPath).toString,
          Await.result(getJavaViaDao(appName, uploadTime, classPath), 3 seconds)
        ).asInstanceOf[JavaJarInfo]
    }else{
      Await.result(getJavaViaDao(appName, uploadTime, classPath), 3 seconds)
    }
  }
}