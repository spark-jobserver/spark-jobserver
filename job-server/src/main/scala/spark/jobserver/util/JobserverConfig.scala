package spark.jobserver.util

object JobserverConfig {

  /**
    * A configuration property for contexts and can be passed during the POST /context.
    * If set, on any error the context would be stopped.
    */
  val STOP_CONTEXT_ON_JOB_ERROR = "stop-context-on-job-error"

}
