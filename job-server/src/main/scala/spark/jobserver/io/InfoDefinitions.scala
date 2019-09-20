package spark.jobserver.io

import org.joda.time.{DateTime, Duration}
import spray.http.{HttpHeaders, MediaType, MediaTypes}

trait BinaryType {
  def extension: String
  def name: String
  def mediaType: MediaType
}

object BinaryType {
  case object Jar extends BinaryType {
    val extension = "jar"
    val name = "Jar"
    val mediaType: MediaType = MediaTypes.register(MediaType.custom("application/java-archive"))
    val contentType = HttpHeaders.`Content-Type`(mediaType)
  }

  case object Egg extends BinaryType {
    val extension = "egg"
    val name = "Egg"
    val mediaType: MediaType = MediaTypes.register(MediaType.custom("application/python-archive"))
    val contentType = HttpHeaders.`Content-Type`(mediaType)
  }

  case object URI extends BinaryType {
    // WARNING: only for internal use (not accepted for upload from user)
    val extension = "uri"
    val name = "Uri"
    val mediaType: MediaType = MediaTypes.register(MediaType.custom("text/uri-list"))
    val contentType = HttpHeaders.`Content-Type`(mediaType)
  }

  def fromString(typeString: String): BinaryType = typeString match {
    case "Jar" => Jar
    case "Egg" => Egg
    case "Uri" => URI
  }

  def fromMediaType(mediaType: MediaType): Option[BinaryType] = mediaType match {
    case m if m == Jar.mediaType => Some(Jar)
    case m if m == Egg.mediaType => Some(Egg)
    case _ => None
  }
}

// Both a response and used to track job progress
// NOTE: if endTime is not None, then the job has finished.
case class JobInfo(jobId: String, contextId: String, contextName: String, mainClass: String, state: String,
                   startTime: DateTime, endTime: Option[DateTime],
                   error: Option[ErrorData], cp: Seq[BinaryInfo]) {

  def jobLengthMillis: Option[Long] = endTime.map { end => new Duration(startTime, end).getMillis }
}

trait ContextUnModifiableAttributes {
  def id: String
  def name: String
  def config: String
  def startTime: DateTime
}

trait ContextModifiableAttributes {
  def actorAddress: Option[String]
  def endTime: Option[DateTime]
  def state: String
  def error: Option[Throwable]
}

case class ContextInfo(id: String, name: String,
                       config: String, actorAddress: Option[String],
                       startTime: DateTime, endTime: Option[DateTime],
                       state: String, error: Option[Throwable])
  extends ContextUnModifiableAttributes with ContextModifiableAttributes with Equals {

  // Meaningful comparison of contextInfos with throwables
  override def hashCode(): Int = {
    val prime = 41
    prime * (prime * (prime * (prime * (prime * (prime * (prime * (prime
      + id.hashCode) + name.hashCode) + config.hashCode) + actorAddress.hashCode)
      + startTime.hashCode) + endTime.hashCode) + state.hashCode) + error.hashCode
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: spark.jobserver.io.ContextInfo => (id == that.id
        && name == that.name && config == that.config
        && actorAddress == that.actorAddress && startTime == that.startTime
        && endTime == that.endTime && state == that.state
        && error.map(e => e.getMessage) == that.error.map(e => e.getMessage))
      case _ => false
    }
  }

}

object ContextInfoModifiable {
  def apply(state: String): ContextInfoModifiable = new ContextInfoModifiable(state)
  def apply(state: String, error: Option[Throwable]): ContextInfoModifiable =
    new ContextInfoModifiable(state, error)

  def getEndTime(state: String): Option[DateTime] = {
    ContextStatus.getFinalStates().contains(state) match {
      case true => Some(DateTime.now())
      case false => None
    }
  }
}

case class ContextInfoModifiable(actorAddress: Option[String],
                                 endTime: Option[DateTime],
                                 state: String,
                                 error: Option[Throwable]) extends ContextModifiableAttributes {
  def this(state: String) = this(None, ContextInfoModifiable.getEndTime(state), state, None)
  def this(state: String, error: Option[Throwable]) =
    this(None, ContextInfoModifiable.getEndTime(state), state, error)
}

// Uniquely identifies the binary used to run a job
case class BinaryInfo(appName: String,
                      binaryType: BinaryType,
                      uploadTime: DateTime,
                      binaryStorageId: Option[String] = None)

object JobStatus {
  val Running = "RUNNING"
  val Error = "ERROR"
  val Finished = "FINISHED"
  val Started = "STARTED"
  val Killed = "KILLED"
  val Restarting = "RESTARTING"
  def getFinalStates(): Seq[String] = Seq(Error, Finished, Killed)
  def getNonFinalStates(): Seq[String] = Seq(Started, Running, Restarting)
}

object ContextStatus {
  val Running = "RUNNING"
  val Error = "ERROR"
  val Stopping = "STOPPING"
  val Finished = "FINISHED"
  val Started = "STARTED"
  val Killed = "KILLED"
  val Restarting = "RESTARTING"
  def getFinalStates(): Seq[String] = Seq(Error, Finished, Killed)
  def getNonFinalStates(): Seq[String] = Seq(Started, Running, Stopping, Restarting)
}
