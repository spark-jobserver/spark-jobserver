package spark.jobserver.util

import spark.jobserver.io.ContextInfo

final case class DeleteBinaryInfoFailedException(private val appName: String) extends
    Exception(s"can't delete meta data information for $appName")

final case class NoStorageIdException(private val appName: String) extends
  Exception(s"can't find hash for $appName in metadata database")

final case class SaveBinaryException(private val appName: String) extends
  Exception(s"can't save binary: $appName in database")

final case class NoSuchBinaryException(private val appName: String)
  extends Exception(s"can't find binary: $appName in database")

final case class ResolutionFailedOnStopContextException(private val context : ContextInfo) extends Exception (
  s"""Could not resolve jobManagerActor (${context.actorAddress})
     | for context ${context.name} during StopContext.""".stripMargin)

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

final case class StoppedContextJoinedBackException() extends
  Exception("Stopped context tried to join the cluster")

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

final case class NoCorrespondingContextAliveException(jobId: String) extends
  Exception(s"No context is alive against running job ($jobId). Cleaning the job.")