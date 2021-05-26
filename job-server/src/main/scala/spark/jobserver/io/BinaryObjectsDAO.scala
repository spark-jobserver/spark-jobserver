package spark.jobserver.io

import java.math.BigInteger

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object BinaryObjectsDAO {
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
trait BinaryObjectsDAO {
  /**
    * Persist a binary file provided through /binaries endpoint.
    *
    * @param id unique binary identifier
    * @param binaryBytes
    * @throws NotImplementedError if not implemented in storage class.
    */
  def saveBinary(id: String,
                 binaryBytes: Array[Byte]): Future[Boolean] = {
    throw new NotImplementedError()
  }

  /**
    * Delete a binary object provided through /binaries endpoint.
    * @param id unique binary identifier
    * @throws NotImplementedError if not implemented in storage class.
    */
  def deleteBinary(id: String): Future[Boolean] = {
    throw new NotImplementedError()
  }

  /**
    * Get a binary object provided through /binaries endpoint.
    * @param id unique binary identifier
    * @throws NotImplementedError if not implemented in storage class.
    */
  def getBinary(id: String): Future[Option[Array[Byte]]] = {
    throw new NotImplementedError()
  }

  /**
   * Persist a binary object which is representing a job result.
   * @param jobId unique identifier of the job that result belongs to
   * @param binaryBytes
   * @throws NotImplementedError if not implemented in storage class.
   */
  def saveJobResult(jobId: String,
                 binaryBytes: Array[Byte]): Future[Boolean] = {
    throw new NotImplementedError()
  }

  /**
   * Delete a binary object which is representing a job result.
   * @param jobId unique identifier of the job that result belongs to
   * @throws NotImplementedError if not implemented in storage class.
   */
  def deleteJobResult(jobId: String): Future[Boolean] = {
    throw new NotImplementedError()
  }

  /**
   * Delete a list of binary objects representing job results.
   * @param jobIds Sequence of unique job identifiers for which results should be deleted
   * @return true if deletion was successful, false otherwise
   * @throws NotImplementedError if not implemented in storage class.
   */
  def deleteJobResults(jobIds: Seq[String]): Future[Boolean] = {
    throw new NotImplementedError()
  }

  /**
   * Get a binary object which is representing a job result.
   * @param jobId unique identifier of the job that result belongs to
   * @throws NotImplementedError if not implemented in storage class.
   */
  def getJobResult(jobId: String): Future[Option[Array[Byte]]] = {
    throw new NotImplementedError()
  }
}
