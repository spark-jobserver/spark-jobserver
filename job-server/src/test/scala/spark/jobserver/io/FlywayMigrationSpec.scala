package spark.jobserver.io

import java.sql.{Connection, DriverManager, ResultSet}

import org.flywaydb.core.Flyway
import org.scalatest.{FunSpec, Matchers}

class ResultSetIterator[T](resultSet: ResultSet, rowHandler: ResultSet => T) extends Iterator[T] {

  var checkNext: Option[Boolean] = Some(resultSet.first())

  override def hasNext: Boolean =
    if(checkNext.isEmpty) {
      val hn = resultSet.next()
      checkNext = Some(hn)
      hn
    } else {
      checkNext.get
    }

  override def next(): T = {
    if(!hasNext) {
      throw new Exception("end of result set")
    } else {
      checkNext = None
      rowHandler(resultSet)
    }
  }
}

object ResultSetIterator {

  def apply[T](resultSet: ResultSet)(rowHandler: ResultSet => T): ResultSetIterator[T] =
    new ResultSetIterator[T](resultSet, rowHandler)
}

class FlywayMigrationSpec extends FunSpec with Matchers {

  def getJDBCConnection(url: String, username: String, password: String, driver: String): Connection = {
    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }

  describe("Flyway scripts for H2 DB") {

    val h2Url = "jdbc:h2:mem:flyway-test;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
    val h2User = ""
    val h2Pass = ""
    val h2Driver = "org.h2.Driver"

    it("should migrate from JAR schema to more generic BINARY schema " +
      "and move raw binaries into BINARIES_CONTENTS") {
      val flyway = new Flyway()
      flyway.setDataSource(h2Url, h2User, h2Pass)
      flyway.setLocations("db/h2/migration/V0_7_0")
      flyway.migrate()
      val sqlConn = getJDBCConnection(h2Url, h2User, h2Pass, h2Driver)
      val statement = sqlConn.createStatement()
      statement.addBatch(
        """INSERT INTO JARS VALUES
          |(1, 'ABC', '2012-01-01', X'deadbeef'),
          |(2, 'DEF', '2012-02-01', X'beadface');
        """.stripMargin)
      statement.addBatch(
        """INSERT INTO JOBS VALUES
          |(1, 'CONTEXT1', 1, 'foo.bar.MyJob', '2012-03-01', '2012-03-02', ''),
          |(2, 'CONTEXT2', 2, 'foo.bar.AnotherJob', '2012-04-01', '2012-04-02', 'OOPS');
        """.stripMargin)
      statement.addBatch(
        """INSERT INTO CONFIGS VALUES
          |(1, 'abc=123'),
          |(2, 'def=456');
        """.stripMargin)
      statement.executeBatch()
      sqlConn.commit()
      flyway.setLocations("db/h2/migration")
      flyway.migrate()

      val descBinaries = sqlConn.createStatement().executeQuery("SHOW COLUMNS FROM BINARIES")
      val descBinariesIt = ResultSetIterator(descBinaries){ r: ResultSet =>
        r.getString("FIELD")
      }
      descBinariesIt.toList should be (List("BIN_ID", "APP_NAME", "UPLOAD_TIME", "BINARY_TYPE", "BIN_HASH"))

      val descBinariesContents = sqlConn.createStatement().executeQuery("SHOW COLUMNS FROM BINARIES_CONTENTS")
      val descBinariesContentsIt = ResultSetIterator(descBinariesContents){ r: ResultSet =>
        r.getString("FIELD")
      }
      descBinariesContentsIt.toList should be (List("BIN_HASH", "BINARY"))

      val descJobs = sqlConn.createStatement().executeQuery("SHOW COLUMNS FROM JOBS")
      val descJobsIt = ResultSetIterator(descJobs){ r: ResultSet =>
        r.getString("FIELD")
      }
      descJobsIt.toList should be (List("JOB_ID", "CONTEXT_NAME", "BIN_ID", "CLASSPATH", "START_TIME", "END_TIME",
          "ERROR", "ERROR_CLASS","ERROR_STACK_TRACE", "CONTEXT_ID", "STATE"))

      val descConfigs = sqlConn.createStatement().executeQuery("SHOW COLUMNS FROM CONFIGS")
      val descConfigsIt = ResultSetIterator(descConfigs){ r: ResultSet =>
        r.getString("FIELD")
      }
      descConfigsIt.toList should be (List("JOB_ID", "JOB_CONFIG"))

      val migratedBinaries =
        sqlConn.createStatement().executeQuery("SELECT BIN_ID, APP_NAME, BINARY_TYPE FROM BINARIES")
      val migratedBinariesIt = ResultSetIterator(migratedBinaries){ r: ResultSet =>
        (r.getInt("BIN_ID"),
          r.getString("APP_NAME"),
          r.getString("BINARY_TYPE"))
      }

      migratedBinariesIt.toList should be (List(
        (1, "ABC", "Jar"),
        (2, "DEF", "Jar")
      ))

      val migratedBinariesContents =
        sqlConn.createStatement().executeQuery("SELECT BIN_HASH, BINARY FROM BINARIES_CONTENTS")
      val migratedBinariesContentsIt = ResultSetIterator(migratedBinariesContents){ r: ResultSet =>
        (r.getString("BIN_HASH"),
          r.getString("BINARY"))
      }
      migratedBinariesContentsIt.toList should be (List(
        ("0000000000000001", "deadbeef"),
        ("0000000000000002", "beadface")
      ))
    }
  }

  /*
    This test is marked as 'ignore' since it requires a running postgres
    instance so it's not truly a unit test. One simple way to run a postgres instance
    supporting this test is to run a postgres docker. The following docker run command matches
    the behaviour of this test:

        docker run --name sjs-postgres -e POSTGRES_PASSWORD=pass -p 5444:5432 -d postgres
   */
  describe("Flyway scripts for Postgres") {

    val pgUrl = "jdbc:postgresql://localhost:5444/postgres"
    val pgUser = "postgres"
    val pgPass = "pass"
    val pgDriver = "org.postgresql.Driver"

    ignore("should migrate from JAR schema to more generic BINARY schema " +
      "and move raw binaries into BINARIES_CONTENTS") {
      val flyway = new Flyway()
      flyway.setDataSource(pgUrl, pgUser, pgPass)
      flyway.clean()
      flyway.setLocations("db/postgresql/migration/V0_7_0")
      flyway.migrate()
      val sqlConn = getJDBCConnection(pgUrl, pgUser, pgPass, pgDriver)
      val statement = sqlConn.createStatement()
      statement.addBatch(
        """INSERT INTO "JARS" VALUES
          |(1, 'ABC', '2012-01-01', X'deadbeef'),
          |(2, 'DEF', '2012-02-01', X'beadface');
        """.stripMargin)
      statement.addBatch(
        """INSERT INTO "JOBS" VALUES
          |(1, 'CONTEXT1', 1, 'foo.bar.MyJob', '2012-03-01', '2012-03-02', ''),
          |(2, 'CONTEXT2', 2, 'foo.bar.AnotherJob', '2012-04-01', '2012-04-02', 'OOPS');
        """.stripMargin)
      statement.addBatch(
        """INSERT INTO "CONFIGS" VALUES
          |(1, 'abc=123'),
          |(2, 'def=456');
        """.stripMargin)
      statement.executeBatch()
      flyway.setLocations("db/postgresql/migration")
      flyway.migrate()
      val descBinaries = sqlConn.getMetaData.getColumns(null, null, "BINARIES", null)
      val descBinariesIt = ResultSetIterator(descBinaries){ r: ResultSet =>
        r.getString("COLUMN_NAME")
      }
      descBinariesIt.toList should be (List("BIN_ID", "APP_NAME", "UPLOAD_TIME", "BINARY", "BINARY_TYPE"))

      val descJobs = sqlConn.getMetaData.getColumns(null, null, "JOBS", null)
      val descJobsIt = ResultSetIterator(descJobs){ r: ResultSet =>
        r.getString("COLUMN_NAME")
      }
      descJobsIt.toList should be (List(
        "JOB_ID", "CONTEXT_NAME", "BIN_ID", "CLASSPATH", "START_TIME", "END_TIME", "ERROR", "CONTEXT_ID", "STATE"))

      val descConfigs = sqlConn.getMetaData.getColumns(null, null, "CONFIGS", null)
      val descConfigsIt = ResultSetIterator(descConfigs){ r: ResultSet =>
        r.getString("COLUMN_NAME")
      }
      descConfigsIt.toList should be (List("JOB_ID", "JOB_CONFIG"))

      val migratedBinaries =
        sqlConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
          executeQuery("SELECT \"BIN_ID\", \"APP_NAME\", \"BINARY_TYPE\" FROM \"BINARIES\"")
      val migratedBinariesIt = ResultSetIterator(migratedBinaries){ r: ResultSet =>
        (r.getInt("BIN_ID"),
          r.getString("APP_NAME"),
          r.getString("BINARY_TYPE"))
      }

      migratedBinariesIt.toList should be (List(
        (1, "ABC", "Jar"),
        (2, "DEF", "Jar")
      ))

      val migratedBinariesContents =
        sqlConn.createStatement().executeQuery("SELECT \"BIN_ID\", \"BINARY\" FROM \"BINARIES_CONTENTS\"")
      val migratedBinariesContentsIt = ResultSetIterator(migratedBinariesContents){ r: ResultSet =>
        (r.getInt("BIN_ID"),
          r.getString("BINARY"))
      }
      migratedBinariesContentsIt.toList should be (List(
        (1, "deadbeef"),
        (2, "beadface")
      ))
    }
  }
}
