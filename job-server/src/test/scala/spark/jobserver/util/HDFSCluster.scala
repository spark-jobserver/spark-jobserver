package spark.jobserver.util

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils

/**
  * Provides a test HDFS Cluster.
  * Attention! MiniDFSCluster can cause unexpected side effects due to sharing of config.
  * For more info: https://issues.apache.org/jira/browse/HDFS-6360
  */
trait HDFSCluster {
  private var hdfsCluster: MiniDFSCluster = _

  def startHDFS(): Unit = {
    val baseDir = new File(PathUtils.getTestDir(getClass), "miniHDFS")
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)

    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.format(true).build()
    hdfsCluster.waitClusterUp()
  }

  def getNameNodeURI(): String = {
    "hdfs://localhost:" + hdfsCluster.getNameNodePort()
  }

  def writeFileToHDFS(srcFilePath: String, hdfsDestPath: String): Unit = {
    val fs = FileSystem.get(getHDFSConf())
    fs.mkdirs(new Path(hdfsDestPath))
    fs.copyFromLocalFile(new Path(srcFilePath), new Path(hdfsDestPath))
  }

  def getHDFSConf(): Configuration = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", getNameNodeURI())
    hadoopConf
  }

  def shutdownHDFS(): Unit = {
    hdfsCluster.shutdown()
  }
}