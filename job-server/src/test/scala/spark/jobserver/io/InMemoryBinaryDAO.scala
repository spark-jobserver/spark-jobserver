package spark.jobserver.io

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class InMemoryBinaryDAO extends BinaryDAO {
  var binaries = mutable.HashMap.empty[String, (Array[Byte])]

  override def save(id: String, binaryBytes: Array[Byte]): Future[Boolean] = Future {
    binaries(id) = binaryBytes
    true
  }

  override def get(id: String): Future[Option[Array[Byte]]] = Future {
    binaries.get(id)
  }

  override def delete(id: String): Future[Boolean] = Future {
    binaries.remove(id) match {
      case None => false
      case Some(_) => true
    }
  }

}
