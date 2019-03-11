package db.h2.migration.V0_7_8_1

import java.sql.Connection

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

import org.flywaydb.core.api.migration.jdbc.JdbcMigration
import org.slf4j.LoggerFactory

import slick.driver.H2Driver.api.actionBasedSQLInterpolation
import spark.jobserver.slick.unmanaged.UnmanagedDatabase

/**
 * To reduce the h2 -> ZK migration workload
 * this JdbcMigration cleans up old and unused metadata.
 *
 * It deletes
 *  - Contexts, which are
 *    + in state FINISHED, ERROR or KILLED (final states)
 *    + AND the end_time is more than 30 days old
 *    + (or in some faulty cases in the past:
 *       which are in final state, the end_time is not set,
 *       but start date is more than 30 days in the past)
 *
 *  - Jobs, for which there is no context in the database
 *
 *  - JobConfigs, for which there is no job in the database
 *
 * In other words: it keeps only the metadata which is associated
 * with a recent context (i.e: running or stopped within the last 30 days)
 *
 */
class V0_7_8_1__delete_old_masterdata extends JdbcMigration {

  protected val timeout = 5 minutes
  protected val logger = LoggerFactory.getLogger(getClass)

  protected def logErrors = PartialFunction[Throwable, Unit] {
    case e: Throwable => logger.error(e.getMessage, e)
  }

  val deleteContexts = sqlu"""DELETE FROM contexts
    WHERE state IN ('FINISHED', 'ERROR', 'KILLED')
    AND (DATEDIFF(DAY, end_time, CURRENT_TIMESTAMP) > 30
    OR (end_time IS NULL AND DATEDIFF(DAY, start_time, CURRENT_TIMESTAMP) > 30))"""
  val deleteJobs = sqlu"""DELETE FROM jobs WHERE context_id NOT IN (SELECT id FROM contexts)"""
  val deleteConfigs = sqlu"""DELETE FROM configs WHERE job_id NOT IN (SELECT job_id FROM jobs)"""

  def migrate(c: Connection): Unit = {
    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false)
    logger.info("Start of old metadata cleanup")

    // Contexts
    logger.info("Cleaning up old contexts")
    try {
      Await.ready(db.run(deleteContexts).recover{logErrors}, timeout)
      c.commit()
    } catch {
      case NonFatal(e) => { c.rollback() }
    }

    // Jobs
    logger.info("Cleaning up old jobs")
    try {
      Await.ready(db.run(deleteJobs).recover{logErrors}, timeout)
      c.commit()
    } catch {
      case NonFatal(e) => { c.rollback() }
    }

    // Configs
    logger.info("Cleaning up old configs")
    try {
      Await.ready(db.run(deleteConfigs).recover{logErrors}, timeout)
      c.commit()
    } catch {
      case NonFatal(e) => { c.rollback() }
    }

  }

}