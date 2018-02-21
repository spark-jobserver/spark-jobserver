package spark.jobserver.util

case class NoSuchBinaryException(private val appName: String) extends Exception {
  private val message = s"can't find binary: $appName in database";
}

final case class InternalServerErrorException(id: String) extends
  Exception(s"Failed to create context ($id) due to internal error")

final case class NoCallbackFoundException(id: String, actorPath: String) extends
  Exception(s"Callback methods not found for actor with id=$id, path=$actorPath")
