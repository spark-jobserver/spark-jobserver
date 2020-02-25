package spark.jobserver.util

object JobserverConfig {

  /**
    * A configuration property for contexts and can be passed during the POST /context.
    * If set, on any error the context would be stopped.
    */
  val STOP_CONTEXT_ON_JOB_ERROR = "stop-context-on-job-error"

  /**
    *  To enable/disable hive support for SparkSession context. The default
    *  in Spark for version 2.3.2 is true where as in 2.4.x it is false.
    *  In jobserver the default is set to true by default.
    *  This is a context specific property.
    */
  val IS_SPARK_SESSION_HIVE_ENABLED = "spark.session.hive.enabled"

  /**
   * Configuration paths to define Binary and Metadata parts of the DAO.
   */
  val BINARY_DAO_CONFIG_PATH = "spark.jobserver.binarydao.class"
  val METADATA_DAO_CONFIG_PATH = "spark.jobserver.metadatadao.class"
  val DAO_ROOT_DIR_PATH = "spark.jobserver.daorootdir"

  /**
   * Classes defining SQL DAO. Used in some checks on startup.
   */
  val BINARY_SQL_DAO_CLASS = "spark.jobserver.io.BinarySqlDAO"
  val METADATA_SQL_DAO_CLASS = "spark.jobserver.io.MetaDataSqlDAO"

  /**
   * Prefix used for naming JobManagerActor. Name is constructed as prefix + context id.
   */
  val MANAGER_ACTOR_PREFIX = "jobManager-"
}
