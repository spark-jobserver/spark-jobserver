package db.migration.V0_7_5

import java.sql.Blob
import java.sql.Connection
import javax.sql.rowset.serial.SerialBlob

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

import org.flywaydb.core.api.migration.jdbc.JdbcMigration
import org.slf4j.Logger
import slick.dbio.DBIO
import slick.dbio.Effect
import slick.dbio.NoStream
import slick.jdbc.GetResult
import slick.jdbc.PositionedParameters
import slick.jdbc.SetParameter
import slick.profile.SqlAction
import spark.jobserver.slick.unmanaged.UnmanagedDatabase
import slick.dbio.DBIOAction
import slick.dbio.Streaming

trait Migration extends JdbcMigration {
  protected val Timeout = 10 minutes
  protected val logger: Logger

  protected case class BinaryContent(id: Int, binary: Blob)

  protected implicit object SetSerialBlob extends SetParameter[SerialBlob] {
    def apply(v: SerialBlob, pp: PositionedParameters) {
      pp.setBlob(v)
    }
  }
  protected def insertBlob(id: Int, blob: SerialBlob): SqlAction[Int, NoStream, Effect]

  protected val createContentsTable: SqlAction[Int, NoStream, Effect]

  protected implicit val getBinaryResult = GetResult[BinaryContent](
      r => BinaryContent(r.nextInt(), r.nextBlob()))
  protected val getBinaryContents: DBIOAction[Seq[(BinaryContent)], Streaming[BinaryContent], Effect]

  protected val dropColumn: SqlAction[Int, NoStream, Effect]

  protected def logErrors = PartialFunction[Throwable, Unit] {
    case e: Throwable => logger.error(e.getMessage, e)
  }

  protected def insertBlob(db: UnmanagedDatabase, b: BinaryContent): Unit = {
    val blob = new SerialBlob(b.binary.getBytes(1, b.binary.length().toInt))
    Await.ready(db.run(insertBlob(b.id, blob)).recover{logErrors}, Timeout)
  }

  def migrate(c: Connection): Unit = {
    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false);
    try {
      Await.ready(
          for {
            _ <- db.run(createContentsTable)
            _ <- db.stream(getBinaryContents).foreach(b => insertBlob(db, b))
            _ <- db.run(dropColumn)
          } yield Unit, Timeout
      ).recover{logErrors}
      c.commit()
    } catch {
      case NonFatal(e) => { c.rollback() }
    }
  }
}

