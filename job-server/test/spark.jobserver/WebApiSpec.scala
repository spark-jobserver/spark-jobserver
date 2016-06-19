package spark.jobserver

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import spark.jobserver.io.{JarInfo, JobDAOActor, JobInfo}
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest


// Tests web response codes and formatting
// Does NOT test underlying Supervisor / JarManager functionality
// HttpService trait is needed for the sealRoute() which wraps exception handling
class WebApiSpec extends FunSpec with Matchers with BeforeAndAfterAll
with ScalatestRouteTest with HttpService {
  import scala.collection.JavaConverters._

  def actorRefFactory: ActorSystem = system

  val bindConfKey = "spark.jobserver.bind-address"
  val bindConfVal = "127.0.0.1"
  val masterConfKey = "spark.master"
  val masterConfVal = "spark://localhost:7077"
  val config = ConfigFactory.parseString(s"""
    spark {
      master = "$masterConfVal"
      jobserver.bind-address = "$bindConfVal"
      jobserver.short-timeout = 3 s
    }
    shiro {
      authentication = off
    }
                                 """)

  val dummyPort = 9999

  // See http://doc.akka.io/docs/akka/2.2.4/scala/actors.html#Deprecated_Variants;
  // for actors declared as inner classes we need to pass this as first arg
  val dummyActor = system.actorOf(Props(classOf[DummyActor], this))
  val jobDaoActor = system.actorOf(JobDAOActor.props(new InMemoryDAO))
  val statusActor = system.actorOf(JobStatusActor.props(jobDaoActor))

  val api = new WebApi(system, config, dummyPort, dummyActor, dummyActor, dummyActor, dummyActor)
  val routes = api.myRoutes

  val dt = DateTime.parse("2013-05-29T00Z")
  val baseJobInfo = JobInfo("foo-1", "context", JarInfo("demo", dt), "com.abc.meme", dt, None, None)
  val finishedJobInfo = baseJobInfo.copy(endTime = Some(dt.plusMinutes(5)))
  val StatusKey = "status"
  val ResultKey = "result"
  class DummyActor extends Actor {

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
      case GetJobStatus("_num") =>
        sender ! finishedJobInfo
      case GetJobStatus("_stream") =>
        sender ! finishedJobInfo
      case GetJobResult("_num") =>
        sender ! JobResult("_num", 5000)
      case GetJobStatus("_unk") =>
        sender ! finishedJobInfo
      case GetJobResult("_unk") => sender ! JobResult("_case", Seq(1, math.BigInt(101)))
      case GetJobStatus("job_to_kill") => sender ! baseJobInfo
      case GetJobStatus(id) => sender ! baseJobInfo
      case GetJobResult(id) => sender ! JobResult(id, id + "!!!")
      case GetJobStatuses(limitOpt) =>
        sender ! Seq(baseJobInfo, finishedJobInfo)

      case ListJars => sender ! Map("demo1" -> dt, "demo2" -> dt.plusHours(1))
      // Ok these really belong to a JarManager but what the heck, type unsafety!!
      case StoreJar("badjar", _) => sender ! InvalidJar
      case StoreJar(_, _)        => sender ! JarStored

      case DataManagerActor.StoreData("/tmp/fileToRemove", _) => sender ! DataManagerActor.Stored("/tmp/fileToRemove-time-stamp")
      case DataManagerActor.StoreData("errorfileToRemove", _) => sender ! DataManagerActor.Error
      case DataManagerActor.ListData => sender ! Set("demo1", "demo2")
      case DataManagerActor.DeleteData("/tmp/fileToRemove") => sender ! DataManagerActor.Deleted
      case DataManagerActor.DeleteData("errorfileToRemove") => sender ! DataManagerActor.Error

      case ListContexts =>  sender ! Seq("context1", "context2")
      case StopContext("none") => sender ! NoSuchContext
      case StopContext(_)      => sender ! ContextStopped
      case AddContext("one", _) => sender ! ContextAlreadyExists
      case AddContext(_, _)     => sender ! ContextInitialized

      case GetContext("no-context") => sender ! NoSuchContext
      case GetContext(_)            => sender ! (self, self)

      case StartAdHocContext(_, _) => sender ! (self, self)

      // These routes are part of JobManagerActor
      case StartJob("no-app", _, _, _)   =>  sender ! NoSuchApplication
      case StartJob(_, "no-class", _, _) =>  sender ! NoSuchClass
      case StartJob("wrong-type", _, _, _) => sender ! WrongJobType
      case StartJob("err", _, config, _) =>  sender ! JobErroredOut("foo", dt,
                                                        new RuntimeException("oops",
                                                          new IllegalArgumentException("foo")))
      case StartJob("foo", _, config, events)     =>
        statusActor ! Subscribe("foo", sender, events)
        val jobInfo = JobInfo("foo", "context", null, "com.abc.meme", dt, None, None)
        statusActor ! JobStatusActor.JobInit(jobInfo)
        statusActor ! JobStarted(jobInfo.jobId, jobInfo)
        val map = config.entrySet().asScala.map { entry => entry.getKey -> entry.getValue.unwrapped }.toMap
        if (events.contains(classOf[JobResult])) sender ! JobResult("foo", map)
        statusActor ! Unsubscribe("foo", sender)

      case StartJob("foo.stream", _, config, events)     =>
        statusActor ! Subscribe("foo.stream", sender, events)
        val jobInfo = JobInfo("foo.stream", "context", null, "", dt, None, None)
        statusActor ! JobStatusActor.JobInit(jobInfo)
        statusActor ! JobStarted(jobInfo.jobId, jobInfo)
        val result = "\"1, 2, 3, 4, 5, 6\"".getBytes().toStream
        if (events.contains(classOf[JobResult])) sender ! JobResult("foo.stream", result)
        statusActor ! Unsubscribe("foo.stream", sender)

      case GetJobConfig("badjobid") => sender ! NoSuchJobId
      case GetJobConfig(_)          => sender ! config
    }
  }
}

