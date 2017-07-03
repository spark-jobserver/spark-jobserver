package db.h2.migration.V0_7_4

import java.sql.Connection

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt

import org.flywaydb.core.api.migration.jdbc.JdbcMigration
import org.slf4j.LoggerFactory

import javax.sql.rowset.serial.SerialBlob
import slick.dbio.DBIO
import slick.driver.H2Driver.api.actionBasedSQLInterpolation
import slick.jdbc.GetResult
import slick.jdbc.PositionedParameters
import slick.jdbc.SetParameter
import spark.jobserver.slick.unmanaged.UnmanagedDatabase
import java.sql.Blob

class V0_7_4__Migrate_Blobs extends JdbcMigration {
  private val Timeout = 10 minutes
  private val logger = LoggerFactory.getLogger(getClass)

  private case class BinaryContent(id: Int, binary: Blob)

  private def logErrors = PartialFunction[Throwable, Unit] {
    case e: Throwable => logger.error(e.getMessage, e)
  }

  private def insertBlob(db: UnmanagedDatabase, b: BinaryContent): Unit = {
    implicit object SetSerialBlob extends SetParameter[SerialBlob] {
      def apply(v: SerialBlob, pp: PositionedParameters) {
        pp.setBlob(v)
      }
    }
    val blob = new SerialBlob(b.binary.getBytes(1, b.binary.length().toInt))
    val insertBlob = sqlu"""INSERT INTO BINARIES_CONTENTS ("BIN_ID", "BINARY") VALUES (${b.id}, ${blob})"""
    Await.ready(db.run(insertBlob).recover{logErrors}, Timeout)
  }

  def migrate(c: Connection): Unit = {
    val createContentsTable = sqlu"""CREATE TABLE BINARIES_CONTENTS (
      BIN_ID  BIGINT  NOT NULL PRIMARY KEY,
      BINARY  BLOB
    );"""
    implicit val getBinaryResult = GetResult[BinaryContent](r => BinaryContent(r.nextInt(), r.nextBlob()))
    val getBinaryContents = sql"""SELECT BIN_ID, BINARY FROM BINARIES""".as[BinaryContent]
    val dropColumn = sqlu"""ALTER TABLE BINARIES DROP COLUMN BINARY"""

    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false)
    Await.ready(
        for {
          _ <- db.run(createContentsTable)
          _ <- db.stream(getBinaryContents).foreach(b => insertBlob(db, b))
          _ <- db.run(dropColumn)
        } yield Unit, Timeout
    ).recover{logErrors}
    c.commit()
  }
}