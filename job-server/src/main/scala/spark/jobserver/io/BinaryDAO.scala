package spark.jobserver.io

import java.math.BigInteger

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object BinaryDAO {
  private val logger = LoggerFactory.getLogger(getClass)

  def calculateBinaryHash(binBytes: Array[Byte]): Array[Byte] = {
    import java.security.MessageDigest
    val md = MessageDigest.getInstance("SHA-256")
    md.digest(binBytes)
  }

  def calculateBinaryHashString(binBytes: Array[Byte]): String = {
    hashBytesToString(calculateBinaryHash(binBytes))
  }

  def hashStringToBytes(binHash: String): Array[Byte] = {
    val bytes = new BigInteger(binHash, 16).toByteArray
    if (bytes.head == 0 ) bytes.tail else bytes // BigInteger returns sign bit
  }

  def hashBytesToString(binHash: Array[Byte]): String = {
    String.format("%032x", new BigInteger(1, binHash))
  }
}

/**
  * Core trait for data access objects for persisting binary data.
  */
trait BinaryDAO {
  /**
    * Persist a binary file.
    *
    * @param id unique binary identifier
    * @param binaryBytes
    * @throws NotImplementedError if not implemented in storage class.
    */
  def save(id: String,
           binaryBytes: Array[Byte]): Future[Boolean] = {
    throw new NotImplementedError()
  }

  /**
    * Delete a binary file.
    * @param id unique binary identifier
    * @throws NotImplementedError if not implemented in storage class.
    */
  def delete(id: String): Future[Boolean] = {
    throw new NotImplementedError()
  }

  /**
    * Get a binary file.
    * @param id unique binary identifier
    * @throws NotImplementedError if not implemented in storage class.
    */
  def get(id: String): Future[Option[Array[Byte]]] = {
    throw new NotImplementedError()
  }
}
