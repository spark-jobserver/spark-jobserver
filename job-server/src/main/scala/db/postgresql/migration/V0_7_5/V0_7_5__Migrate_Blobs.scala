package db.postgresql.migration.V0_7_5

import java.sql.Blob
import java.sql.Connection
import javax.sql.rowset.serial.SerialBlob

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

import db.migration.V0_7_5.Migration
import org.flywaydb.core.api.migration.jdbc.JdbcMigration
import org.slf4j.LoggerFactory
import slick.dbio.DBIO
import slick.dbio.Effect
import slick.dbio.NoStream
import slick.driver.PostgresDriver.api.actionBasedSQLInterpolation
import slick.jdbc.GetResult
import slick.jdbc.PositionedParameters
import slick.jdbc.SetParameter
import slick.sql.SqlAction
import spark.jobserver.slick.unmanaged.UnmanagedDatabase

class V0_7_5__Migrate_Blobs extends Migration {
  val logger = LoggerFactory.getLogger(getClass)

  protected def insertBlob(id: Int, blob: SerialBlob): SqlAction[Int, NoStream, Effect] = {
    sqlu"""INSERT INTO "BINARIES_CONTENTS" ("BIN_ID", "BINARY") VALUES (${id}, ${blob})"""
  }
  val createContentsTable = sqlu"""CREATE TABLE "BINARIES_CONTENTS" (
    "BIN_ID"  SERIAL  NOT NULL PRIMARY KEY,
    "BINARY"  OID
  );"""
  val getBinaryContents = sql"""SELECT "BIN_ID", "BINARY" FROM "BINARIES"""".as[BinaryContent]
  val dropColumn = sqlu"""ALTER TABLE "BINARIES" DROP COLUMN "BINARY""""

  override def migrate(c: Connection): Unit = {
    val createTriggerBinariesContents = sqlu"""CREATE TRIGGER t_binary
            BEFORE UPDATE OR DELETE ON "BINARIES_CONTENTS"
            FOR EACH ROW EXECUTE PROCEDURE lo_manage("BINARY")"""
    val dropTriggerBinaries = sqlu"""DROP TRIGGER t_binary ON "BINARIES""""
    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false);
    try {
      Await.ready(
          for {
            _ <- db.run(createContentsTable)
            _ <- db.run(createTriggerBinariesContents)
            _ <- db.stream(getBinaryContents).foreach(b => insertBlob(db, b))
            _ <- db.run(dropColumn)
            _ <- db.run(dropTriggerBinaries)
          } yield Unit, Timeout
      ).recover{logErrors}
      c.commit()
    } catch {
      case NonFatal(e) => { c.rollback() }
    }
  }
}

