package spark.jobserver.util

import java.io.{File, InputStreamReader}
import java.net.URI

import org.apache.commons.io.IOUtils.toByteArray
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils

class HDFSCluster extends HDFSClusterLike

trait HDFSClusterLike {
  private var hdfsCluster: MiniDFSCluster = null

  def startHDFS(): Unit = {
    val baseDir = new File(PathUtils.getTestDir(getClass()), "miniHDFS")
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())

    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.nameNodePort(8020).format(true).build()
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

  def fileExists(filePath: String): Boolean = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(getHDFSConf())
    fs.exists(path)
  }

  def cleanHDFS(dir: String): Unit = {
    val path = new Path(dir)
    val fs = path.getFileSystem(getHDFSConf())
    val t = fs.delete(path, true)
  }

  def readFile(filePath: String): Array[Byte] = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(getHDFSConf())
    val t = fs.open(path)
    toByteArray(t)
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