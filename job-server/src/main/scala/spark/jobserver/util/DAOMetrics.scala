package spark.jobserver.util

import java.util.concurrent.TimeUnit

import spark.jobserver.common.akka.metrics.YammerMetrics

object DAOMetrics extends YammerMetrics {
  // Counters
  val totalSuccessfulSaveRequests = counter("total-save-binary-success")
  val totalFailedSaveBinaryDAORequests = counter("total-binary-save-binary-dao-failed")
  val totalFailedSaveMetadataDAORequests = counter("total-binary-save-metadata-dao-failed")
  val totalSuccessfulDeleteRequests = counter("total-delete-binary-success")
  val totalFailedDeleteBinaryDAORequests = counter("total-binary-delete-binary-dao-failed")
  val totalFailedDeleteMetadataDAORequests = counter("total-binary-delete-metadata-dao-failed")

  // Timer metrics
  val binList = timer("binary-list-duration", TimeUnit.MILLISECONDS)
  val binRead = timer("binary-read-duration", TimeUnit.MILLISECONDS)
  val binWrite = timer("binary-write-duration", TimeUnit.MILLISECONDS)
  val binDelete = timer("binary-delete-duration", TimeUnit.MILLISECONDS)
  val contextList = timer("context-list-duration", TimeUnit.MILLISECONDS)
  val contextRead = timer("context-read-duration", TimeUnit.MILLISECONDS)
  val contextQuery = timer("context-query-duration", TimeUnit.MILLISECONDS)
  val contextWrite = timer("context-write-duration", TimeUnit.MILLISECONDS)
  val jobList = timer("job-list-duration", TimeUnit.MILLISECONDS)
  val jobRead = timer("job-read-duration", TimeUnit.MILLISECONDS)
  val jobQuery = timer("job-query-duration", TimeUnit.MILLISECONDS)
  val jobWrite = timer("job-write-duration", TimeUnit.MILLISECONDS)
  val configRead = timer("config-read-duration", TimeUnit.MILLISECONDS)
  val configWrite = timer("config-write-duration", TimeUnit.MILLISECONDS)
}
