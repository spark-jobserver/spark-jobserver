package spark.jobserver.util

import akka.actor.{Actor, ActorRef}
import org.joda.time.DateTime

import spark.jobserver.JobManagerActor.JobKilledException
import spark.jobserver._
import spark.jobserver.io._
import scala.collection.JavaConverters._

import com.typesafe.config.Config

class DummyActor(statusActor: ActorRef, config: Config) extends Actor {
  val dt = DateTime.parse("2013-05-29T00Z")
  val baseJobInfo =
    JobInfo("foo-1", "context", BinaryInfo("demo", BinaryType.Jar, dt), "com.abc.meme", dt, None, None)
  val finishedJobInfo = baseJobInfo.copy(endTime = Some(dt.plusMinutes(5)))
  val errorJobInfo    = finishedJobInfo.copy(error = Some(new Throwable("test-error")))
  val killedJobInfo   = finishedJobInfo.copy(error = Some(JobKilledException(finishedJobInfo.jobId)))
  val JobId           = "jobId"
  val StatusKey       = "status"
  val ResultKey       = "result"
  import CommonMessages._
  import ContextSupervisor._
  import JobInfoActor._
  import JobManagerActor._

  def receive: PartialFunction[Any, Unit] = {
    case GetJobStatus("_mapseq") =>
      sender ! finishedJobInfo
    case GetJobResult("_mapseq") =>
      val map = Map("first" -> Seq(1, 2, Seq("a", "b")))
      sender ! JobResult("_seqseq", map)
    case GetJobStatus("_mapmap") =>
      sender ! finishedJobInfo
    case GetJobResult("_mapmap") =>
      val map = Map("second" -> Map("K" -> Map("one" -> 1)))
      sender ! JobResult("_mapmap", map)
    case GetJobStatus("_seq") =>
      sender ! finishedJobInfo
    case GetJobResult("_seq") =>
      sender ! JobResult("_seq", Seq(1, 2, Map("3" -> "three")))
    case GetJobResult("_stream") =>
      sender ! JobResult("_stream", "\"1, 2, 3, 4, 5, 6, 7\"".getBytes().toStream)
    case GetJobStatus("_stream") =>
      sender ! finishedJobInfo
    case GetJobStatus("_num") =>
      sender ! finishedJobInfo
    case GetJobResult("_num") =>
      sender ! JobResult("_num", 5000)
    case GetJobStatus("_unk") =>
      sender ! finishedJobInfo
    case GetJobResult("_unk") => sender ! JobResult("_case", Seq(1, math.BigInt(101)))
    case GetJobStatus("_running") =>
      sender ! baseJobInfo
    case GetJobResult("_running") =>
      sender ! baseJobInfo
    case GetJobStatus("_finished") =>
      sender ! finishedJobInfo
    case GetJobStatus("_no_status") =>
      sender ! NoSuchJobId
    case GetJobStatus("job_to_kill") => sender ! baseJobInfo
    case GetJobStatus(id)            => sender ! baseJobInfo
    case GetJobResult(id)            => sender ! JobResult(id, id + "!!!")
    case GetJobStatuses(limitOpt, statusOpt) => {
      statusOpt match {
        case Some(JobStatus.Error)    => sender ! Seq(errorJobInfo)
        case Some(JobStatus.Killed)   => sender ! Seq(killedJobInfo)
        case Some(JobStatus.Finished) => sender ! Seq(finishedJobInfo)
        case Some(JobStatus.Running)  => sender ! Seq(baseJobInfo)
        case _                        => sender ! Seq(baseJobInfo, finishedJobInfo)
      }
    }

    case ListBinaries(Some(BinaryType.Jar)) =>
      sender ! Map("demo1" -> (BinaryType.Jar, dt), "demo2" -> (BinaryType.Jar, dt.plusHours(1)))

    case ListBinaries(_) =>
      sender ! Map(
        "demo1" -> (BinaryType.Jar, dt),
        "demo2" -> (BinaryType.Jar, dt.plusHours(1)),
        "demo3" -> (BinaryType.Egg, dt.plusHours(2))
      )
    // Ok these really belong to a JarManager but what the heck, type unsafety!!
    case StoreBinary("badjar", _, _) => sender ! InvalidBinary
    case StoreBinary("daofail", _, _) =>
      sender ! BinaryStorageFailure(new Exception("DAO failed to store"))
    case StoreBinary(_, _, _) => sender ! BinaryStored

    case DeleteBinary(_) => sender ! BinaryDeleted

    case DataManagerActor.StoreData("errorfileToRemove", _) => sender ! DataManagerActor.Error
    case DataManagerActor.StoreData(filename, _) => {
      sender ! DataManagerActor.Stored(filename + "-time-stamp")
    }
    case DataManagerActor.ListData                        => sender ! Set("demo1", "demo2")
    case DataManagerActor.DeleteData("/tmp/fileToRemove") => sender ! DataManagerActor.Deleted
    case DataManagerActor.DeleteData("errorfileToRemove") => sender ! DataManagerActor.Error

    case ListContexts                   => sender ! Seq("context1", "context2")
    case StopContext("none")            => sender ! NoSuchContext
    case StopContext("timeout-ctx")     => sender ! ContextStopError(new Throwable)
    case StopContext(_)                 => sender ! ContextStopped
    case AddContext("one", _)           => sender ! ContextAlreadyExists
    case AddContext("custom-ctx", c)    => sender ! ContextInitialized
    case AddContext("initError-ctx", _) => sender ! ContextInitError(new Throwable)
    case AddContext(_, _)               => sender ! ContextInitialized

    case GetContext("no-context") => sender ! NoSuchContext
    case GetContext(_)            => sender ! (self, self)

    case StartAdHocContext(_, _) => sender ! (self, self)

    // These routes are part of JobManagerActor
    case StartJob("no-app", _, _, _)     => sender ! NoSuchApplication
    case StartJob(_, "no-class", _, _)   => sender ! NoSuchClass
    case StartJob("wrong-type", _, _, _) => sender ! WrongJobType
    case StartJob("err", _, config, _) =>
      sender ! JobErroredOut("foo", dt, new RuntimeException("oops", new IllegalArgumentException("foo")))
    case StartJob("foo", _, config, events) =>
      statusActor ! Subscribe("foo", sender, events)
      val jobInfo = JobInfo("foo", "context", null, "com.abc.meme", dt, None, None)
      statusActor ! JobStatusActor.JobInit(jobInfo)
      statusActor ! JobStarted(jobInfo.jobId, jobInfo)
      val map = config
        .entrySet()
        .asScala
        .map { entry =>
          entry.getKey -> entry.getValue.unwrapped
        }
        .toMap
      if (events.contains(classOf[JobResult])) sender ! JobResult("foo", map)
      statusActor ! Unsubscribe("foo", sender)

    case StartJob("foo.stream", _, config, events) =>
      statusActor ! Subscribe("foo.stream", sender, events)
      val jobInfo = JobInfo("foo.stream", "context", null, "", dt, None, None)
      statusActor ! JobStatusActor.JobInit(jobInfo)
      statusActor ! JobStarted(jobInfo.jobId, jobInfo)
      val result = "\"1, 2, 3, 4, 5, 6\"".getBytes().toStream
      if (events.contains(classOf[JobResult])) sender ! JobResult("foo.stream", result)
      statusActor ! Unsubscribe("foo.stream", sender)

    case GetJobConfig("badjobid") => sender ! NoSuchJobId
    case GetJobConfig(_)          => sender ! config

    case StoreJobConfig(_, _) => sender ! JobConfigStored
    case KillJob(jobId)       => sender ! JobKilled(jobId, DateTime.now())
  }
}
