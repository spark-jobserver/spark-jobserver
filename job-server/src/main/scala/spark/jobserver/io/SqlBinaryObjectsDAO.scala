package spark.jobserver.io

import java.sql.Blob

import com.typesafe.config.Config
import javax.sql.rowset.serial.SerialBlob
import org.slf4j.LoggerFactory
import slick.driver.JdbcProfile
import spark.jobserver.util.SqlDBUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe

class SqlBinaryObjectsDAO(config: Config) extends BinaryObjectsDAO {
  private val logger = LoggerFactory.getLogger(getClass)
  val dbUtils = new SqlDBUtils(config)

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profile = runtimeMirror.reflectModule(dbUtils.profileModule).instance.asInstanceOf[JdbcProfile]

  import profile.api._

  dbUtils.initFlyway()

  //scalastyle:off
  class Blobs(tag: Tag) extends Table[(String, Blob)](tag, "BLOBS") {
    def binId = column[String]("BIN_ID", O.PrimaryKey)
    def binary = column[Blob]("BINARY")
    def * = (binId, binary)
  }
  val blobs = TableQuery[Blobs]
  //scalastyle:on

  /**
   * Persist a binary file.
   *
   * @param id unique binary identifier
   * @param binaryBytes
   */
  override def saveBinary(id: String,
                          binaryBytes: Array[Byte]): Future[Boolean] = {
    val dbAction = blobs.insertOrUpdate((id, new SerialBlob(binaryBytes)))
    dbUtils.db.run(dbAction).map(_ > 0).recover(dbUtils.logDeleteErrors)
  }

  /**
   * Delete a binary file.
   * @param id unique binary identifier
   */
  override def deleteBinary(id: String): Future[Boolean] = {
    val dbAction = blobs.filter(_.binId === id).delete
    dbUtils.db.run(dbAction).map(_ > 0).recover(dbUtils.logDeleteErrors)
  }

  /**
   * Get a binary file.
   * @param id unique binary identifier
   */
  override def getBinary(id: String): Future[Option[Array[Byte]]] = {
    val dbAction = blobs.filter(_.binId === id).map(_.binary).result
    dbUtils.db.run(dbAction.headOption.map {
      case Some(b) => Some(b.getBytes(1, b.length.toInt))
      case None =>
        logger.info(s"Binary with $id is not found")
        None
    }.transactionally)
  }
}
