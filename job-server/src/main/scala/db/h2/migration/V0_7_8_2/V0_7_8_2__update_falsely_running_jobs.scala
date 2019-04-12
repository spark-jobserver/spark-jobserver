package db.h2.migration.V0_7_8_2

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
 * Due to a bug there are jobs in the database, which
 * - are in a non-final state
 * - are bound to a context which is in a final state.
 *
 * A recent fix prohibiting the deletion of running jobs
 * will falsely fail because of these jobs.
 *
 * This migration updates all these jobs to the ERROR state.
 *
 */
class V0_7_8_2__update_falsely_running_jobs extends JdbcMigration {

  protected val timeout = 5 minutes
  protected val logger = LoggerFactory.getLogger(getClass)

  protected def logErrors = PartialFunction[Throwable, Unit] {
    case e: Throwable => logger.error(e.getMessage, e)
  }

  val fixJobs = sqlu"""UPDATE jobs
    SET state = 'ERROR'
    WHERE job_id IN(
      SELECT job_id
      FROM jobs j INNER JOIN contexts c ON j.context_id = c.id
      WHERE j.state IN('STARTED','RUNNING','RESTARTING')
      AND c.state IN('ERROR','FINISHED','KILLED')
    )"""

  def migrate(c: Connection): Unit = {
    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false)

    // Query
    logger.info("Setting non-final job states with a final context state to ERROR")
    try {
      Await.ready(db.run(fixJobs).recover{logErrors}, timeout)
      c.commit()
    } catch {
      case NonFatal(e) => { c.rollback() }
    }

  }

}