package spark.jobserver.util

import com.typesafe.config.Config
import javax.sql.DataSource
import org.apache.commons.dbcp.BasicDataSource
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import slick.driver.JdbcProfile

import scala.reflect.runtime.universe

class SqlDBUtils(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)

  // DB initialization
  private val slickDriverClass = config.getString(s"spark.jobserver.sqldao.slick-driver")
  private val jdbcDriverClass = config.getString(s"spark.jobserver.sqldao.jdbc-driver")
  private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  val profileModule = runtimeMirror.staticModule(slickDriverClass)
  val profile = runtimeMirror.reflectModule(profileModule).instance.asInstanceOf[JdbcProfile]
  import profile.api._

  private val jdbcUrl = config.getString(s"spark.jobserver.sqldao.jdbc.url")
  private val jdbcUser = config.getString(s"spark.jobserver.sqldao.jdbc.user")
  private val jdbcPassword = config.getString(s"spark.jobserver.sqldao.jdbc.password")
  private val enableDbcp = config.getBoolean(s"spark.jobserver.sqldao.dbcp.enabled")
  val db = if (enableDbcp) {
    logger.info("DBCP enabled")
    val dbcpMaxActive = config.getInt(s"spark.jobserver.sqldao.dbcp.maxactive")
    val dbcpMaxIdle = config.getInt(s"spark.jobserver.sqldao.dbcp.maxidle")
    val dbcpInitialSize = config.getInt(s"spark.jobserver.sqldao.dbcp.initialsize")
    val dataSource: DataSource = {
      val ds = new BasicDataSource
      ds.setDriverClassName(jdbcDriverClass)
      ds.setUsername(jdbcUser)
      ds.setPassword(jdbcPassword)
      ds.setMaxActive(dbcpMaxActive)
      ds.setMaxIdle(dbcpMaxIdle)
      ds.setInitialSize(dbcpInitialSize)
      ds.setUrl(jdbcUrl)
      ds
    }
    Database.forDataSource(dataSource, None)
  } else {
    logger.info("DBCP disabled")
    Database.forURL(jdbcUrl, driver = jdbcDriverClass, user = jdbcUser, password = jdbcPassword)
  }

  def initFlyway() {
    val migrateLocations = config.getString("flyway.locations")
    val initOnMigrate = config.getBoolean("flyway.initOnMigrate")

    // Flyway migration
    val flyway = new Flyway()
    flyway.setDataSource(jdbcUrl, jdbcUser, jdbcPassword)
    flyway.setLocations(migrateLocations)
    flyway.setBaselineOnMigrate(initOnMigrate)
    flyway.migrate()
  }

  def logDeleteErrors: PartialFunction[Any, Boolean] = {
    case e: Throwable =>
      logger.error(e.getMessage, e)
      false
  }

}
