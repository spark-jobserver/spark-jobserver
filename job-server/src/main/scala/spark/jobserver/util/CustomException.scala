package spark.jobserver.util

case class NoSuchBinaryException(private val appName: String) extends Exception {
  private val message = s"can't find binary: $appName in database";
}
