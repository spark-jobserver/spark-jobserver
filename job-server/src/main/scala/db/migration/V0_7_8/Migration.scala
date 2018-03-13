package db.migration.V0_7_8

import java.sql.Connection

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

import org.flywaydb.core.api.migration.jdbc.JdbcMigration
import org.slf4j.Logger

import slick.jdbc.GetResult
import slick.profile.SqlAction
import slick.dbio.DBIOAction
import slick.dbio.Streaming
import slick.dbio.Effect
import slick.dbio.NoStream
import spark.jobserver.slick.unmanaged.UnmanagedDatabase
import spark.jobserver.io.{BinaryInfo, JobStatus, ErrorData}

trait Migration extends JdbcMigration {
  protected val timeout = 10 minutes
  protected val logger: Logger

  protected def logErrors = PartialFunction[Throwable, Unit] {
    case e: Throwable => logger.error(e.getMessage, e)
  }

  protected case class JobData(id: String, endTime: Option[Object], error: Option[Object])
  protected def insertState(id: String, status: String): SqlAction[Int, NoStream, Effect]

  protected val addContextId: SqlAction[Int, NoStream, Effect]
  protected val updateContextId: SqlAction[Int, NoStream, Effect]
  protected val setContextIdNotNull: SqlAction[Int, NoStream, Effect]
  protected val addState: SqlAction[Int, NoStream, Effect]
  implicit val getJobsEndTimeAndErrorResult = GetResult[JobData](r =>
      JobData(r.nextString(), r.nextObjectOption(), r.nextObjectOption()))
  protected val getJobsEndTimeAndError: DBIOAction[Seq[(JobData)], Streaming[JobData], Effect]
  protected val setStateNotNull: SqlAction[Int, NoStream, Effect]

  protected def setState(db: UnmanagedDatabase, jobData: JobData): Unit = {
    val status = jobData match {
      case JobData(_, None, _) => JobStatus.Running
      case JobData(_, _, Some(err)) => JobStatus.Error
      case JobData(_, Some(e), None) => JobStatus.Finished
    }
    Await.ready(db.run(insertState(jobData.id, status)).recover{logErrors}, timeout)
  }

  def migrate(c: Connection): Unit = {
    val db = new UnmanagedDatabase(c)
    c.setAutoCommit(false)
    try {
      Await.ready(
          for {
            _ <- db.run(addContextId)
            _ <- db.run(updateContextId)
            _ <- db.run(setContextIdNotNull)
            _ <- db.run(addState)
            _ <- db.stream(getJobsEndTimeAndError).foreach(j => setState(db, j))
          } yield Unit, timeout
      ).recover{logErrors}
      Await.ready(db.run(setStateNotNull).recover{logErrors}, timeout)
      c.commit()
    } catch {
      case NonFatal(e) => { c.rollback() }
    }
  }
}