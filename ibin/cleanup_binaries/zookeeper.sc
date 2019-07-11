import $ivy.`org.apache.curator:apache-curator:2.6.0`
import $ivy.`org.apache.curator:curator-framework:2.6.0`

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.utils.ZKPaths
import scala.util.control.NonFatal
import scala.io.Source
import java.io._

// Fix type mismatch: java.util.List[String] in curator results
import scala.collection.JavaConversions._

val baseFolder = "jobserver/db"

def getClient: CuratorFramework = {
  val client = CuratorFrameworkFactory.builder.
    connectString("localhost:9999").
    retryPolicy(new RetryNTimes(2, 1000)).
    namespace(baseFolder).
    connectionTimeoutMs(2350).
    sessionTimeoutMs(10000).
    build
  client.start()
  client
}

def list(client: CuratorFramework, dir: String): Seq[String] = {
  if (client.checkExists().forPath(dir) == null) {
    Seq.empty[String]
  } else {
    client.getChildren.forPath(dir).toList
  }
}

def delete(client: CuratorFramework, dir: String): Boolean = {
  try {
    if (client.checkExists().forPath(dir) != null) {
      ZKPaths.deleteChildren(client.getZookeeperClient.getZooKeeper,
        ZKPaths.fixForNamespace(baseFolder, dir),
        false)
      client.delete().forPath(dir)
    }
    true
  } catch {
    case NonFatal(e) =>
      false
  }
}

def listAllBinaries(): List[String] = {
  val client = getClient
  try {
    list(client, "binaries").toList
  } finally {
    client.close()
  }
}

def filterBinariesToDelete(binariesInCF: List[String], zookeeperBinaries: List[String]): List[String] = {
  zookeeperBinaries.filter(bin => !binariesInCF.contains(bin))
}

def getBinariesFromFile(filename: String = ""): List[String] = {
  val source = Source.fromFile(filename)
  try {
    source.getLines.toList.map(_.replaceAll("^\\s+", ""))
  } finally {
    source.close()
  }
}

def saveBinariesToFile(binaries: List[String], filename: String): Boolean = {
  val writer = new BufferedWriter(new FileWriter(filename))
  try {
    binaries.foreach(libName => writer.write(s"$libName\n"))
    true
  } finally {
    writer.close()
  }
}

@main
def main(zkBinariesFileToSave: String = "",
         cfBinariesFile: String = "",
         binariesToDeleteFile: String = ""): Unit = {
  if (cfBinariesFile != "" && zkBinariesFileToSave != "" && binariesToDeleteFile != "") {
    val cfBinaries = getBinariesFromFile(cfBinariesFile)
    val zkBinaries = getBinariesFromFile(zkBinariesFileToSave)
    val binToDelete = filterBinariesToDelete(cfBinaries, zkBinaries)
    saveBinariesToFile(binToDelete, binariesToDeleteFile)
  } else if (zkBinariesFileToSave != "") {
    println("Getting list of Zookeeper binaries..")
    val zkBinaries = listAllBinaries()
    saveBinariesToFile(zkBinaries, zkBinariesFileToSave)
  } else if (binariesToDeleteFile != "") {
    val binariesToDelete = getBinariesFromFile(binariesToDeleteFile)
    val client = getClient
    try {
      binariesToDelete.map(binName => delete(client, s"binaries/$binName"))
    } finally {
      client.close()
    }
  } else
  {
    throw new Exception("Unknown combination of parameters for the script!")
  }

}
