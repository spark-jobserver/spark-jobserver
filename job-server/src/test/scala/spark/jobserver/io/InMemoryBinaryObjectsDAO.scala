package spark.jobserver.io

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class InMemoryBinaryObjectsDAO extends BinaryObjectsDAO {
  var binaries = mutable.HashMap.empty[String, Array[Byte]]
  var jobResults = mutable.HashMap.empty[String, Array[Byte]]

  override def saveBinary(id: String, binaryBytes: Array[Byte]): Future[Boolean] = Future {
    binaries(id) = binaryBytes
    true
  }

  override def getBinary(id: String): Future[Option[Array[Byte]]] = Future {
    binaries.get(id)
  }

  override def deleteBinary(id: String): Future[Boolean] = Future {
    binaries.remove(id) match {
      case None => false
      case Some(_) => true
    }
  }

  override def saveJobResult(jobId: String, binaryBytes: Array[Byte]): Future[Boolean] = Future {
    jobResults(jobId) = binaryBytes
    true
  }

  override def getJobResult(jobId: String): Future[Option[Array[Byte]]] = Future {
    jobResults.get(jobId)
  }

  override def deleteJobResults(jobIds: Seq[String]): Future[Boolean] = Future {
    jobIds.foreach(jobId => jobResults.remove(jobId))
    true
  }
}
