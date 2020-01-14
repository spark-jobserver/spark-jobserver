package db.mysql.migration.V0_7_9

import java.sql.Connection

import org.flywaydb.core.api.migration.jdbc.JdbcMigration
import org.slf4j.LoggerFactory
import slick.driver.PostgresDriver.api.actionBasedSQLInterpolation
import slick.jdbc.GetResult
import spark.jobserver.slick.unmanaged.UnmanagedDatabase

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

/**
 * Jobs now can be started from multiple binaries and/or multiple uris. This migration therefore
 * adds two new columns to JobInfo table: bin_ids and uris. Additionally data from old bin_id column
 * is migrated to bin_ids column.
 */
class V0_7_9__jobinfo_contains_uris extends JdbcMigration{
  private val logger = LoggerFactory.getLogger(getClass)
  private val timeout = 10 minutes
  private def logErrors = PartialFunction[Throwable, Unit] {
    e: Throwable => logger.error(e.getMessage, e)
  }

  private case class JobBinInfo(jobId: String, binId: String)

  protected def insertBinIDList(db: UnmanagedDatabase,
                                bins: String, jobId: String): Unit = {
    val updateBin = sqlu"""UPDATE `JOBS` SET `BIN_IDS`=$bins WHERE `JOB_ID`=$jobId"""
    Await.ready(db.run(updateBin).recover{logErrors}, timeout)
  }

  override def migrate(c: Connection): Unit = {
    implicit val getBinaryIdForJobResult = GetResult[JobBinInfo](
      r => JobBinInfo(r.nextString(), r.nextInt().toString))
    val getBinaryIdForJob = sql"""SELECT `JOB_ID`, `BIN_ID` FROM `JOBS`""".as[JobBinInfo]
    val createBinIdsColumn = sqlu"""ALTER TABLE `JOBS` ADD COLUMN `BIN_IDS` TEXT"""
    val createURIsColumn = sqlu"""ALTER TABLE `JOBS` ADD COLUMN `URIS` TEXT"""
    val dropColumn = sqlu"""ALTER TABLE `JOBS` DROP COLUMN `BIN_ID`"""


    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false)
    try {
      Await.ready(
        for {
          _ <- db.run(createBinIdsColumn)
          _ <- db.run(createURIsColumn)
          _ <- db.stream(getBinaryIdForJob).foreach(
            j => insertBinIDList(db, j.jobId, j.binId))
          _ <- db.run(dropColumn)
        } yield Unit, timeout
      ).recover{logErrors}
      c.commit()
    } catch {
      case NonFatal(e) => {
        logger.error(s"Error during database migration (update JobInfo): ${e.getMessage}")
        c.rollback()
      }
    }
  }
}
