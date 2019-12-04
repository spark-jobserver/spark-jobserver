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

}
