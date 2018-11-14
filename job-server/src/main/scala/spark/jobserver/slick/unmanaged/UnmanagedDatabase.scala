package spark.jobserver.slick.unmanaged

import java.sql.Connection

import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.BaseSession
import slick.jdbc.JdbcBackend.DatabaseDef
import slick.jdbc.JdbcDataSource
import slick.util.AsyncExecutor

class UnmanagedDataSource(conn: Connection) extends JdbcDataSource {
  def createConnection(): Connection = conn
  def close(): Unit = ()
  override val maxConnections: Option[Int] = Some(100)
}

class UnmanagedSession(database: DatabaseDef) extends BaseSession(database) {
  override def close(): Unit = ()
}

/**
 * Slick database wrapper for a JDBC connection
 *
 * @param conn the JDBC connection to wrap
 * @return database wrapper
 */
class UnmanagedDatabase(conn: Connection) extends JdbcBackend.DatabaseDef(
    new UnmanagedDataSource(conn), AsyncExecutor("UmanagedDatabase-Executor", 1, -1)) {
  override def createSession(): JdbcBackend.Session = new UnmanagedSession(this)
}