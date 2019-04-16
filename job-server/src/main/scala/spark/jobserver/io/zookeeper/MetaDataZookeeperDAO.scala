package spark.jobserver.io.zookeeper

import com.typesafe.config.Config
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.common.akka.metrics.YammerMetrics
import spark.jobserver.io._
import spark.jobserver.util.JsonProtocols
import spark.jobserver.util.Utils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework

object MetaDataZookeeperDAO {
  private val binariesDir = "/binaries"
  private val contextsDir = "/contexts"
  private val jobsDir = "/jobs"
}

class MetaDataZookeeperDAO(config: Config) extends MetaDataDAO with YammerMetrics {
  import MetaDataZookeeperDAO._
  import JsonProtocols._

  private val logger = LoggerFactory.getLogger(getClass)
  private val zookeeperUtils = new ZookeeperUtils(config)

  // Timer metrics
  private val binList = timer("zk-binary-list-duration", TimeUnit.MILLISECONDS)
  private val binRead = timer("zk-binary-read-duration", TimeUnit.MILLISECONDS)
  private val binQuery = timer("zk-binary-query-duration", TimeUnit.MILLISECONDS)
  private val binWrite = timer("zk-binary-write-duration", TimeUnit.MILLISECONDS)
  private val binDelete = timer("zk-binary-delete-duration", TimeUnit.MILLISECONDS)
  private val contextList = timer("zk-context-list-duration", TimeUnit.MILLISECONDS)
  private val contextRead = timer("zk-context-read-duration", TimeUnit.MILLISECONDS)
  private val contextQuery = timer("zk-context-query-duration", TimeUnit.MILLISECONDS)
  private val contextWrite = timer("zk-context-write-duration", TimeUnit.MILLISECONDS)
  private val jobList = timer("zk-job-list-duration", TimeUnit.MILLISECONDS)
  private val jobRead = timer("zk-job-read-duration", TimeUnit.MILLISECONDS)
  private val jobQuery = timer("zk-job-query-duration", TimeUnit.MILLISECONDS)
  private val jobWrite = timer("zk-job-write-duration", TimeUnit.MILLISECONDS)
  private val configRead = timer("zk-config-read-duration", TimeUnit.MILLISECONDS)
  private val configWrite = timer("zk-config-write-duration", TimeUnit.MILLISECONDS)

  /*
   * Contexts
   */

  override def saveContext(contextInfo: ContextInfo): Future[Boolean] = {
    logger.debug(s"Saving context ${contextInfo.id} (state: ${contextInfo.state})")
    Utils.timedFuture(contextWrite) {
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            val id = contextInfo.id
            zookeeperUtils.write(client, contextInfo, s"$contextsDir/$id")
        }
      }
    }
  }

  override def getContext(id: String): Future[Option[ContextInfo]] = {
    logger.debug(s"Retrieving context $id")
    Utils.timedFuture(contextRead) {
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            val path = s"$contextsDir/$id"
            zookeeperUtils.sync(client, path)
            zookeeperUtils.read[ContextInfo](client, path) match {
              case Some(contextInfo) =>
                Some(contextInfo)
              case None =>
                logger.info(s"Didn't find any context information for the path: $path")
                None
            }
        }
      }
    }
  }

  override def getContextByName(name: String): Future[Option[ContextInfo]] = {
    logger.debug(s"Retrieving context by name $name")
    Utils.timedFuture(contextQuery) {
      Future {
          Utils.usingResource(zookeeperUtils.getClient) {
            client =>
              zookeeperUtils.sync(client, contextsDir)
              zookeeperUtils.list(client, contextsDir)
                .flatMap(id => zookeeperUtils.read[ContextInfo](client, s"$contextsDir/$id"))
                .filter(_.name == name)
                .sortBy(_.startTime.getMillis)
                .lastOption
          }
      }
    }
  }

  override def getContexts(limit: Option[Int], statuses: Option[Seq[String]]): Future[Seq[ContextInfo]] = {
    logger.debug("Retrieving multiple contexts")
    Utils.timedFuture(contextList) {
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.sync(client, contextsDir)
            lazy val allContexts = zookeeperUtils.list(client, contextsDir)
              .flatMap(id => zookeeperUtils.read[ContextInfo](client, s"$contextsDir/$id"))
            lazy val filteredContexts = statuses match {
              case None => allContexts
              case Some(allowed) => allContexts.filter(c => allowed.contains(c.state))
            }
            val sortedContexts = filteredContexts
              .sortBy(_.startTime.getMillis)(Ordering[Long].reverse)
            limit match {
              case None =>
                sortedContexts
              case Some(l) =>
                sortedContexts.take(l)
            }
        }
      }
    }
  }

  /*
   * Jobs
   */

  override def saveJob(jobInfo: JobInfo): Future[Boolean] = {
    logger.debug(s"Saving job ${jobInfo.jobId} (state: ${jobInfo.state})")
    Utils.timedFuture(jobWrite){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            val jobId = jobInfo.jobId
            zookeeperUtils.write(client, jobInfo, s"$jobsDir/$jobId")
        }
      }
    }
  }

  override def getJob(id: String): Future[Option[JobInfo]] = {
    logger.debug(s"Retrieving job $id")
    Utils.timedFuture(jobRead){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.sync(client, s"$jobsDir/$id")
            readJobInfo(client, id)
        }
      }
    }
  }

  override def getJobs(limit: Int, status: Option[String]): Future[Seq[JobInfo]] = {
    logger.debug("Retrieving multiple jobs")
    Utils.timedFuture(jobList){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.sync(client, jobsDir)
            lazy val allJobs = zookeeperUtils.list(client, jobsDir)
              .flatMap(id => readJobInfo(client, id))
            lazy val filteredJobs =
              status match {
                case None => allJobs
                case Some(allowed) => allJobs.filter(_.state == allowed)
              }
            filteredJobs.sortBy(_.startTime.getMillis)(Ordering[Long].reverse)
              .take(limit)
        }
      }
    }
  }

  override def getJobsByContextId(contextId: String, statuses: Option[Seq[String]]): Future[Seq[JobInfo]] = {
    logger.debug(s"Retrieving jobs by contextId $contextId")
    Utils.timedFuture(jobQuery){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.sync(client, jobsDir)
            lazy val allJobs = zookeeperUtils.list(client, jobsDir)
              .flatMap(id => readJobInfo(client, id))
              .filter(_.contextId == contextId)
            lazy val filteredJobs =
              statuses match {
                case None => allJobs
                case Some(allowed) => allJobs.filter(j => allowed.contains(j.state))
              }
            filteredJobs.sortBy(_.startTime.getMillis)(Ordering[Long].reverse)
        }
      }
    }
  }

  /*
   * Helpers
   */

  private def readJobInfo(client: CuratorFramework, jobId: String): Option[JobInfo] = {
    zookeeperUtils.read[JobInfo](client, s"$jobsDir/$jobId") match {
      case None =>
        logger.info(s"Didn't find any job information for the path: $jobsDir/$jobId")
        None
      case Some(j) => {
        Some(j)
      }
    }

  }

  /*
   * JobConfigs
   */

  override def saveJobConfig(jobId: String, config: Config): Future[Boolean] = {
    logger.debug(s"Saving job config for job $jobId")
    Utils.timedFuture(configWrite){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            val configRender = config.root().render(ConfigRenderOptions.concise())
            zookeeperUtils.write(client, configRender, s"$jobsDir/$jobId/config")
        }
      }
    }
  }

  override def getJobConfig(jobId: String): Future[Option[Config]] = {
    logger.debug(s"Retrieving job config for $jobId")
    Utils.timedFuture(configRead){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            val path = s"$jobsDir/$jobId/config"
            zookeeperUtils.sync(client, path)
            zookeeperUtils.read[String](client, path)
              .flatMap(render => Some(ConfigFactory.parseString(render)))
        }
      }
    }
  }

  /*
   * Binaries
   */

  override def getBinary(name: String): Future[Option[BinaryInfo]] = {
    logger.debug(s"Retrieving binary $name")
    Utils.timedFuture(binRead){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            val path = s"$binariesDir/$name"
            zookeeperUtils.sync(client, path)
            zookeeperUtils.read[Seq[BinaryInfo]](client, path) match {
              case Some(infoForBinary) =>
                Some(infoForBinary.sortWith(_.uploadTime.getMillis > _.uploadTime.getMillis).head)
              case None =>
                None
            }
        }
      }
    }
  }

  override def getBinaries: Future[Seq[BinaryInfo]] = {
    logger.debug("Retrieving all binaries")
    Utils.timedFuture(binList){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.sync(client, binariesDir)
            zookeeperUtils.list(client, binariesDir).map(
              name => zookeeperUtils.read[Seq[BinaryInfo]](client, s"$binariesDir/$name"))
              .filter(_.isDefined)
              .map(binary => binary.get.head)
        }
      }
    }
  }

  override def getBinariesByStorageId(storageId: String): Future[Seq[BinaryInfo]] = {
    logger.debug(s"Retrieving binaries for storage id $storageId")
    Utils.timedFuture(binQuery){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.sync(client, binariesDir)
            zookeeperUtils.list(client, binariesDir).flatMap(
              name => zookeeperUtils.read[Seq[BinaryInfo]](client, s"$binariesDir/$name")
              .getOrElse(Seq.empty[BinaryInfo]))
              .filter(_.binaryStorageId.getOrElse("") == storageId)
              .groupBy(_.appName).map(_._2.head).toSeq
        }
      }
    }
  }

  override def saveBinary(name: String, binaryType: BinaryType,
                          uploadTime: DateTime, binaryStorageId: String): Future[Boolean] = {
    logger.debug(s"Saving binary $name")
    Utils.timedFuture(binWrite){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            val oldInfoForBinary = zookeeperUtils.read[Seq[BinaryInfo]](client, s"$binariesDir/$name")
            val newBinary = BinaryInfo(name, binaryType, uploadTime, Some(binaryStorageId))
            zookeeperUtils.write[Seq[BinaryInfo]](client,
              newBinary +: oldInfoForBinary.getOrElse(Seq.empty[BinaryInfo]),
              s"$binariesDir/$name")
        }
      }
    }
  }

  override def deleteBinary(name: String): Future[Boolean] = {
    logger.debug(s"Deleting binary $name")
    Utils.timedFuture(binDelete){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.read[Seq[BinaryInfo]](client, s"$binariesDir/$name") match {
              case Some(_) =>
                zookeeperUtils.delete(client, s"$binariesDir/$name")
              case None =>
                logger.info(s"Didn't find any information for the path: $binariesDir/$name")
                false
            }
        }
      }
    }
  }

  /**
    * Gets the jobs by binary name.
    *
    * The behavior of this function differs from other dao implementations.
    * In other dao's, if user deletes a binary and then tries to fetch the
    * associated job then jobserver will not return anything. This is due to
    * the join between Jobs & binaries table. This behavior is not correct.
    *
    * Whereas in Zookeeper dao, user can still get the jobs even if the
    * binaries are deleted.
    * @param binName Name of binary
    * @param statuses List of job statuses
    * @return Sequence of all job infos matching query
    */
  override def getJobsByBinaryName(binName: String, statuses: Option[Seq[String]] = None):
      Future[Seq[JobInfo]] = {
    Utils.timedFuture(jobQuery){
      Future {
        Utils.usingResource(zookeeperUtils.getClient) {
          client =>
            zookeeperUtils.sync(client, binariesDir)
            lazy val jobsUsingBinName = zookeeperUtils.list(client, jobsDir)
                                          .flatMap(id => readJobInfo(client, id))
                                          .filter(_.binaryInfo.appName == binName)
            statuses match {
              case None =>
                jobsUsingBinName
              case Some(states) =>
                jobsUsingBinName.filter(j => states.contains(j.state))
            }
        }
      }
    }
  }
}
