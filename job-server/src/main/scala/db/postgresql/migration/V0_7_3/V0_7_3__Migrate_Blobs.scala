package db.postgresql.migration.V0_7_3

import java.sql.Connection

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt

import org.flywaydb.core.api.migration.jdbc.JdbcMigration
import org.slf4j.LoggerFactory

import javax.sql.rowset.serial.SerialBlob
import slick.dbio.DBIO
import slick.driver.PostgresDriver.api.actionBasedSQLInterpolation
import slick.jdbc.GetResult
import slick.jdbc.PositionedParameters
import slick.jdbc.SetParameter
import spark.jobserver.slick.unmanaged.UnmanagedDatabase

class V0_7_3__Migrate_Blobs extends JdbcMigration {
  private val Timeout = 10 minutes
  private val logger = LoggerFactory.getLogger(getClass)

  private case class BinArray(id: Int, binary: Array[Byte])

  private def logErrors : PartialFunction[Throwable, Unit] = {
    case e: Throwable => logger.error(e.getMessage, e)
  }

  private def insertBlob(db: UnmanagedDatabase, b: BinArray): Unit = {
    implicit object SetSerialBlob extends SetParameter[SerialBlob] {
      def apply(v: SerialBlob, pp: PositionedParameters) {
        pp.setBlob(v)
      }
    }
    val blob = new SerialBlob(b.binary)
    val updateBlob = sqlu"""UPDATE "BINARIES" SET "BINARY"=${blob} WHERE "BIN_ID"=${b.id}"""
    Await.ready(db.run(updateBlob).recover{logErrors}, Timeout)
  }

  def migrate(c: Connection): Unit = {
    val renameBinaryToBinArray = sqlu"""ALTER TABLE "BINARIES" RENAME COLUMN "BINARY" TO "BINARRAY""""
    val addBlobColumn = sqlu"""ALTER TABLE "BINARIES" ADD COLUMN "BINARY" OID"""
    val cleanupTrigger = sqlu"""CREATE TRIGGER t_binary BEFORE UPDATE OR DELETE ON "BINARIES"
                        FOR EACH ROW EXECUTE PROCEDURE lo_manage("BINARY")"""
    val dropBinArray = sqlu"""ALTER TABLE "BINARIES" DROP COLUMN "BINARRAY""""
    implicit val getBinArrayResult = GetResult[BinArray](r => BinArray(r.nextInt(), r.nextBytes()))
    val getBinArrays = sql"""SELECT "BIN_ID", "BINARRAY" FROM "BINARIES"""".as[BinArray]

    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false);
    Await.ready(
        for {
          _ <- db.run(renameBinaryToBinArray)
          _ <- db.run(addBlobColumn)
          _ <- db.run(cleanupTrigger)
          _ <- db.stream(getBinArrays).foreach(b => insertBlob(db, b))
          _ <- db.run(dropBinArray)
        } yield Unit, Timeout
    ).recover{logErrors}
    c.commit()
  }
}