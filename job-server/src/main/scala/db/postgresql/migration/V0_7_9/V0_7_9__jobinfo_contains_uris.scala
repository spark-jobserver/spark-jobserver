package db.postgresql.migration.V0_7_9

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

  override def migrate(c: Connection): Unit = {
    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false)
    try {
      Await.ready(
        for {
          _ <- db.run(sqlu"""CREATE TEMP TABLE jobs_copy as SELECT "JOB_ID", "BIN_ID" FROM "JOBS";""")
          _ <- db.run(sqlu"""ALTER TABLE "JOBS" DROP COLUMN "BIN_ID";""")
          _ <- db.run(sqlu"""ALTER TABLE "JOBS" ADD COLUMN "BIN_IDS" TEXT, ADD COLUMN "URIS" TEXT;""")
          _ <- db.run(sqlu"""UPDATE "JOBS" SET "URIS" = '' WHERE "URIS" is null;""")
          _ <- db.run(sqlu"""UPDATE "JOBS" SET "BIN_IDS" =
                              (SELECT "BIN_ID" FROM jobs_copy WHERE jobs_copy."JOB_ID" = "JOBS"."JOB_ID");""")
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
