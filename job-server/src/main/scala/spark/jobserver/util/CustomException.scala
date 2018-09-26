package spark.jobserver.util

case class NoSuchBinaryException(private val appName: String) extends Exception {
  private val message = s"can't find binary: $appName in database";
}

final case class InternalServerErrorException(id: String) extends
  Exception(s"Failed to create context ($id) due to internal error")

final case class NoCallbackFoundException(id: String, actorPath: String) extends
  Exception(s"Callback methods not found for actor with id=$id, path=$actorPath")

final case class NoJobConfigFoundException(jobId: String) extends
  Exception(s"Failed to load config for job with id: $jobId")

final case class UnexpectedMessageReceivedException(jobId: String) extends
  Exception(s"Received unexpected message for job $jobId")

final case class ContextJVMInitializationTimeout() extends
  Exception("Context failed to connect back within initialization time")

final case class ContextReconnectFailedException() extends
  Exception("Reconnect failed after Jobserver restart")

final case class ContextForcefulKillTimeout() extends
  Exception("Forceful kill for a context failed within deletion time")

final case class NotStandaloneModeException() extends
  Exception("Tried to do a standalone mode only action for other mode")

final case class NoAliveMasterException() extends
  Exception("Could not find alive spark master")

final case class NoMatchingDAOObjectException() extends Exception("No matching dao object found")

final case class ContextKillingItselfException(msg: String) extends Exception(msg)

final case class NoIPAddressFoundException() extends
  Exception("The IP address of this host could not be resolved automatically")

final case class UnsupportedNetworkAddressStrategy(name: String) extends
  Exception(s"Unsupported network address strategy $name specified.")
