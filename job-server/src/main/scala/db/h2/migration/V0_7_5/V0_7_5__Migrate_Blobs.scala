package db.h2.migration.V0_7_5

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
import slick.driver.H2Driver.api.actionBasedSQLInterpolation
import slick.jdbc.GetResult
import slick.jdbc.PositionedParameters
import slick.jdbc.SetParameter
import slick.profile.SqlAction
import spark.jobserver.slick.unmanaged.UnmanagedDatabase

class V0_7_5__Migrate_Blobs extends Migration {
  val logger = LoggerFactory.getLogger(getClass)

  protected def insertBlob(id: Int, blob: SerialBlob): SqlAction[Int, NoStream, Effect] = {
    sqlu"""INSERT INTO "BINARIES_CONTENTS" ("BIN_ID", "BINARY") VALUES (${id}, ${blob})"""
  }
  val createContentsTable = sqlu"""CREATE TABLE "BINARIES_CONTENTS" (
    "BIN_ID"  BIGINT  NOT NULL PRIMARY KEY,
    "BINARY"  BLOB
  );"""
  val getBinaryContents = sql"""SELECT "BIN_ID", "BINARY" FROM "BINARIES"""".as[BinaryContent]
  val dropColumn = sqlu"""ALTER TABLE "BINARIES" DROP COLUMN "BINARY""""
}

