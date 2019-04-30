package spark.jobserver.io

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe
import com.typesafe.config.Config
import slick.driver.JdbcProfile

class SqlTestHelpers(config: Config) {
  private val sqlCommon: SqlCommon = new SqlCommon(config)
  private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  private val profile = runtimeMirror.reflectModule(sqlCommon.profileModule).
    instance.asInstanceOf[JdbcProfile]
  import profile.api._

  def cleanupMetadataTables(): Future[Int] = {
    for {
      bins <- sqlCommon.db.run(sqlCommon.binaries.delete)
      cont <- sqlCommon.db.run(sqlCommon.contexts.delete)
      jobs <- sqlCommon.db.run(sqlCommon.jobs.delete)
      conf <- sqlCommon.db.run(sqlCommon.configs.delete)
    } yield {
      bins + cont + jobs + conf
    }
  }

  def cleanupBinariesContentsTable(config: Config): Future[Int] = {
    sqlCommon.db.run(new JobSqlDAO(config, sqlCommon).binariesContents.delete)
  }
}
