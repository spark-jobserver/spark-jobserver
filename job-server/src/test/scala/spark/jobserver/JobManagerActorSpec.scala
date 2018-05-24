package spark.jobserver

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import akka.actor.{Props, ActorRef, ActorIdentity, ReceiveTimeout, PoisonPill}
import akka.testkit.TestProbe
import org.joda.time.DateTime

import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.DataManagerActor.RetrieveData
import spark.jobserver.JobManagerActor.KillJob
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.CommonMessages.{Subscribe, JobStarted, JobRestartFailed, JobValidationFailed}
import spark.jobserver.io.{BinaryType, JobDAOActor, JobStatus, JobInfo, BinaryInfo}
import spark.jobserver.ContextSupervisor.{AddContext, ContextInitialized}
import spark.jobserver.util.NoJobConfigFoundException

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

object JobManagerActorSpec extends JobSpecConfig

object JobManagerActorSpy {
  def props(daoActor: ActorRef, supervisorActorAddress: String,
      initializationTimeout: FiniteDuration,
      contextId: String, spyProbe: TestProbe, restartCandidates: Int = 0): Props =
    Props(classOf[JobManagerActorSpy], daoActor, supervisorActorAddress,
        initializationTimeout, contextId, spyProbe, restartCandidates)
}
class JobManagerActorSpy(daoActor: ActorRef, supervisorActorAddress: String,
    initializationTimeout: FiniteDuration, contextId: String, spyProbe: TestProbe,
    restartCandidates: Int = 0)
    extends JobManagerActor(daoActor, supervisorActorAddress, contextId, initializationTimeout) {

  totalJobsToRestart = restartCandidates

  def stubbedWrappedReceive: Receive = {
    case msg @ JobStarted(jobId, jobInfo) =>
      spyProbe.ref ! msg

    case msg @ JobRestartFailed(jobId, err) => {
      handleJobRestartFailure(jobId, err, msg)
      spyProbe.ref ! msg
    }

    case msg @ JobValidationFailed(jobId, dateTime, err)  => {
      handleJobRestartFailure(jobId, err, msg)
      spyProbe.ref ! msg
    }
  }

  override def wrappedReceive: Receive = {
    stubbedWrappedReceive.orElse(super.wrappedReceive)
  }

  override protected def sendStartJobMessage(receiverActor: ActorRef, msg: JobManagerActor.StartJob) {
    spyProbe.ref ! "StartJob Received"
    receiverActor ! msg
  }
}

class JobManagerActorSpec extends JobSpecBase(JobManagerActorSpec.getNewSystem) {
  import akka.testkit._
  import CommonMessages._
  import JobManagerActorSpec.MaxJobsPerContext
  import scala.concurrent.duration._

  val classPrefix = "spark.jobserver."
  private val wordCountClass = classPrefix + "WordCountExample"
  private val newWordCountClass = classPrefix + "WordCountExampleNewApi"
  private val javaJob = classPrefix + "JavaHelloWorldJob"
  val sentence = "The lazy dog jumped over the fish"
  val counts = sentence.split(" ").groupBy(x => x).mapValues(_.length)
  protected val stringConfig = ConfigFactory.parseString(s"input.string = $sentence")
  protected val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val contextId = java.util.UUID.randomUUID().toString()

  val initMsgWait = 10.seconds.dilated
  val startJobWait = 5.seconds.dilated

  var contextConfig: Config = _

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    contextConfig = JobManagerActorSpec.getContextConfig(adhoc = false)
    manager = system.actorOf(JobManagerActor.props(daoActor, "", contextId, 40.seconds))
  }

  after {
    AkkaTestUtils.shutdownAndWait(manager)
    Option(supervisor).foreach(AkkaTestUtils.shutdownAndWait(_))
  }

  describe("starting jobs") {
    it("should start job and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job and return result before job finish event") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgClass(classOf[JobResult])
      expectMsgClass(classOf[JobFinished])
      expectNoMsg()
    }

    it("should start job more than one time and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()

      // should be ok to run the same more again
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMsg()
    }

    it("should start NewAPI job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", newWordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMsg()
    }

    it("should start job and return JobStarted (async)") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, errorEvents ++ asyncEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectNoMsg()
    }

    it("should start job, return JobStarted (async) and write context id and status to DAO") {
      import scala.concurrent.Await
      import spark.jobserver.io.JobStatus

      val configWithCtxId = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(configWithCtxId, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()

      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, errorEvents ++ asyncEvents)

      expectMsgClass(startJobWait, classOf[JobStarted])
      val jobInfo = Await.result(dao.getJobInfosByContextId(contextId), 5.seconds)
      jobInfo should not be (None)
      jobInfo.length should be (1)
      jobInfo.head.contextId should be (contextId)
      jobInfo.head.state should be (JobStatus.Running)
      expectNoMsg()
    }

    it("should return error if job throws an error") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "MyErrorJob", emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[RuntimeException])
    }

    it("job should get jobConfig passed in to StartJob message") {
      val jobConfig = ConfigFactory.parseString("foo.bar.baz = 3")
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "ConfigCheckerJob", jobConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, keys: Seq[_]) =>
          keys should contain ("foo")
      }
    }

    it("should properly serialize case classes and other job jar classes") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "ZookeeperJob", stringConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Array[Product]) =>
          result.length should equal (1)
          result(0).getClass.getName should include ("Animal")
      }
      expectNoMsg()
    }

    it ("should refuse to start a job when too many jobs in the context are running") {
      val jobSleepTimeMillis = 2000L
      val jobConfig = ConfigFactory.parseString("sleep.time.millis = " + jobSleepTimeMillis)

      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()

      val messageCounts = new mutable.HashMap[Class[_], Int].withDefaultValue(0)
      // Try to start 3 instances of this job. 2 of them should start, and the 3rd should be denied.
      for (i <- 0 until MaxJobsPerContext + 1) {
        manager ! JobManagerActor.StartJob("demo", classPrefix + "SleepJob", jobConfig, allEvents)
      }

      while (messageCounts.values.sum < (MaxJobsPerContext * 3 + 1)) {
        expectMsgPF(5.seconds.dilated, "Expected a message but didn't get one!") {
          case started: JobStarted =>
            messageCounts(started.getClass) += 1
          case noSlots: NoJobSlotsAvailable =>
            noSlots.maxJobSlots should equal (MaxJobsPerContext)
            messageCounts(noSlots.getClass) += 1
          case finished: JobFinished =>
            messageCounts(finished.getClass) += 1
          case result: JobResult =>
            result.result should equal (jobSleepTimeMillis)
            messageCounts(result.getClass) += 1
        }
      }
      messageCounts.toMap should equal (Map(classOf[JobStarted] -> MaxJobsPerContext,
        classOf[JobFinished] -> MaxJobsPerContext,
        classOf[JobResult] -> MaxJobsPerContext,
        classOf[NoJobSlotsAvailable] -> 1))
    }

    it("should start a job that's an object rather than class") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Int) => result should equal (1 + 2 + 3)
      }
    }

    it("should be able to cancel running job") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "LongPiJob", stringConfig, allEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobStarted(id, _) =>
          manager ! KillJob(id)
          // we need this twice as we send both to sender and manager, in unit tests they are the same
          // in usage they may be different
          expectMsgClass(classOf[JobKilled])
          expectMsgClass(classOf[JobKilled])
      }
    }

    it("should fail a job that requires job jar dependencies but doesn't provide the jar"){
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      val jobJarDepsConfigs = ConfigFactory.parseString(
        s"""
           |dependent-jar-uris = []
        """.stripMargin)

      manager ! JobManagerActor.StartJob("demo", classPrefix + "jobJarDependenciesJob", jobJarDepsConfigs,
        syncEvents ++ errorEvents)

      expectMsgClass(startJobWait, classOf[JobErroredOut])
    }

    it("jobs should be able to cache RDDs and retrieve them through getPersistentRDDs") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "CacheSomethingJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum: Int) = expectMsgClass(classOf[JobResult])

      manager ! JobManagerActor.StartJob("demo", classPrefix + "AccessCacheJob", emptyConfig,
        errorEvents ++ syncEvents)
      val JobResult(_, sum2: Int) = expectMsgClass(classOf[JobResult])

      sum2 should equal (sum)
    }

    it ("jobs should be able to cache and retrieve RDDs by name") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "CacheRddByNameJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgPF(2 seconds, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, sum: Int) => sum should equal (1 + 4 + 9 + 16 + 25)
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
    }
  }

  describe("error conditions") {
    it("should return errors if appName does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo2", wordCountClass, emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchApplication)
    }

    it("should return error message if classPath does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should error out if loading garbage jar") {
      uploadBinary(dao, "../README.md", "notajar", BinaryType.Jar)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("notajar", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should error out if job validation fails") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, emptyConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMsg()
    }

    it("should error out if new API job validation fails") {
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", newWordCountClass, emptyConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMsg()
    }
  }

  describe("kill-context-on-supervisor-down feature tests") {
    it("should not kill itself if kill-context-on-supervisor-down is disabled") {
      manager = system.actorOf(JobManagerActor.props(daoActor, "", contextId, 1.seconds.dilated))
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      managerProbe.expectNoMsg(2.seconds.dilated)
    }

    it("should kill itself if response to Identify message is not received when kill-context-on-supervisor-down is enabled") {
      manager = system.actorOf(JobManagerActor.props(daoActor, "fake-path", contextId, 1.seconds.dilated))
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      managerProbe.expectTerminated(manager, 2.seconds.dilated)
    }

    it("should kill itself if the master is down") {
      val dataManagerActor = system.actorOf(Props.empty)

      // A valid actor which responds to Identify message sent by JobManagerActor
      supervisor = system.actorOf(
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor), "context-supervisor")
      manager = system.actorOf(JobManagerActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}", contextId, 3.seconds.dilated))

      val supervisorProbe = TestProbe()
      supervisorProbe.watch(supervisor)

      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      Thread.sleep(2000) // Wait for manager actor to initialize and add a watch
      supervisor ! PoisonPill

      supervisorProbe.expectTerminated(supervisor)
      managerProbe.expectTerminated(manager, 4.seconds.dilated)
    }

    it("should kill itself if Initialize message is not received") {
      val dataManagerActor = system.actorOf(Props.empty)

      supervisor = system.actorOf(
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor), "context-supervisor")
      manager = system.actorOf(JobManagerActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}", contextId, 2.seconds.dilated))

      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      // supervisor not sending a message is equal to message not received in manager

      // Since, ReceiveTimeout and ActorIdentity are internal messages, there is no
      // direct way to verify them. We can only verify the effect that it gets killed.
      managerProbe.expectTerminated(manager, 3.seconds.dilated)
    }

    it("should not kill itself if Initialize message is received") {
      val dataManagerActor = system.actorOf(Props.empty)

      supervisor = system.actorOf(
          Props(classOf[LocalContextSupervisorActor], TestProbe().ref, dataManagerActor), "context-supervisor")
      manager = system.actorOf(JobManagerActor.props(daoActor,
          s"${supervisor.path.address.toString}${supervisor.path.toStringWithoutAddress}", contextId, 3.seconds.dilated))
      // Wait for Identify/ActorIdentify message exchange
      Thread.sleep(2000)
      val managerProbe = TestProbe()
      managerProbe.watch(manager)

      manager ! JobManagerActor.Initialize(contextConfig, None, TestProbe().ref)

      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      managerProbe.expectNoMsg(4.seconds.dilated)
    }
  }

  describe("remote file cache") {
    it("should support local copy, fetch from remote and cache file") {
      val testData = "test-data".getBytes
      val dataFileActor = TestProbe()

      manager ! JobManagerActor.Initialize(contextConfig, None, dataFileActor.ref)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()

      // use already existing file
      val existingFile = File.createTempFile("test-existing-file-", ".dat")
      Files.write(existingFile.toPath, testData, StandardOpenOption.SYNC)
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob",
        ConfigFactory.parseString(s"testFile = ${existingFile.getAbsolutePath}"),
        errorEvents ++ syncEvents)
      dataFileActor.expectNoMsg()
      val existingFileResult = expectMsgPF() {
        case JobResult(_, fileName: String) => fileName
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      existingFileResult should equal (existingFile.getAbsolutePath)

      // downloads new file from data file manager
      val jobConfig = ConfigFactory.parseString("testFile = test-file")
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob", jobConfig,
        errorEvents ++ syncEvents)
      dataFileActor.expectMsgPF() {
        case RetrieveData(fileName, jobManager) => {
          fileName should equal ("test-file")
          jobManager should equal (manager)
        }
      }
      dataFileActor.reply(DataManagerActor.Data("test-data".getBytes))
      val cachedFile = expectMsgPF() {
        case JobResult(_, fileName: String) => fileName
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      val cachedFilePath = new File(cachedFile).toPath
      Files.exists(cachedFilePath) should equal (true)
      Files.readAllBytes(cachedFilePath) should equal ("test-data".getBytes)

      // uses cached version in second run
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob", jobConfig,
        errorEvents ++ syncEvents)
      dataFileActor.expectNoMsg() // already cached
      val secondResult = expectMsgPF() {
        case JobResult(_, fileName: String) => fileName
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
      cachedFile should equal (secondResult)

      manager ! JobManagerActor.DeleteData("test-file") // cleanup
    }

    it("should fail if remote data file does not exists") {
      val testData = "test-data".getBytes
      val dataFileActor = TestProbe()

      manager ! JobManagerActor.Initialize(contextConfig, None, dataFileActor.ref)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()

      val jobConfig = ConfigFactory.parseString("testFile = test-file")
      manager ! JobManagerActor.StartJob("demo", classPrefix + "RemoteDataFileJob", jobConfig,
        errorEvents ++ syncEvents)

      // return error from file manager
      dataFileActor.expectMsgClass(classOf[RetrieveData])
      dataFileActor.reply(DataManagerActor.Error("test"))

      expectMsgClass(classOf[JobErroredOut])
    }
  }

  describe("Supervise mode unit tests") {
    it("should kill itself if, the only restart candidate job failed") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, "", TestProbe(), 1))
      deathWatcher.watch(manager)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectTerminated(manager)
    }

    it("should kill itself if, all restart candidates failed") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, "", TestProbe(), 3))
      deathWatcher.watch(manager)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobValidationFailed("dummy-id1", DateTime.now(), new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobRestartFailed("dummy-id2", new Exception(""))
      deathWatcher.expectTerminated(manager)
    }

    it("should not kill itself if atleast one job was restarted") {
      val deathWatcher = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, "", TestProbe(), 3))
      deathWatcher.watch(manager)

      val dummyJob = JobInfo("dummy-id2", "", "", BinaryInfo("dummy", BinaryType.Jar, DateTime.now()), "",
          JobStatus.Running, DateTime.now(), None, None)

      manager ! JobRestartFailed("dummy-id", new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobStarted("dummy-id2", dummyJob)
      deathWatcher.expectNoMsg(2.seconds)

      manager ! JobValidationFailed("dummy-id2", DateTime.now(), new Exception(""))
      deathWatcher.expectNoMsg(2.seconds)
    }

    it("should not do anything if no context was found with specified name") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))
      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectNoMsg()
    }

    it("should kill itself if an exception occurred by accessing DAO") {
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, spyProbe))
      managerWatcher.watch(manager)

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      // Not replying to GetJobInfosByContextId will result in a timeout
      // and failure will occur
      managerWatcher.expectTerminated(manager, 5.seconds)
    }

    it("should kill itself if an unexpected message is received from DAO") {
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val contextId = "dummy-context"
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, spyProbe))
      managerWatcher.watch(manager)

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply("Nobody gonna save you now!")
      managerWatcher.expectTerminated(manager)
    }

    it("should not do anything if context was found but no restart candidate jobs") {
      val contextId = "dummy-context"
      val spyProbe = TestProbe()
      val daoProbe = TestProbe()
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, spyProbe))

      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply(JobDAOActor.JobInfos(Seq()))
      spyProbe.expectNoMsg()
    }

    it("should not restart a job if job config is not found in DAO") {
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val binaryInfo = BinaryInfo("dummy", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, "test-class", JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, TestProbe()))
      managerWatcher.watch(manager)
      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply(JobDAOActor.JobInfos(Seq(jobInfo)))

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobConfig])
      daoProbe.reply(JobDAOActor.JobConfig(None))

      daoProbe.expectMsgPF(3.seconds.dilated, "store error in DAO since config not found") {
        case jobInfo: JobDAOActor.SaveJobInfo =>
          jobInfo.jobInfo.state should be(JobStatus.Error)
          jobInfo.jobInfo.error.get.message should be((NoJobConfigFoundException("jobId")).getMessage)
      }

      managerWatcher.expectTerminated(manager)
    }

    it("should not restart a job if there was an error while fetching job config") {
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val managerWatcher = TestProbe()
      val binaryInfo = BinaryInfo("dummy", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, "test-class", JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoProbe.ref, "", 5.seconds, contextId, TestProbe()))
      managerWatcher.watch(manager)
      manager ! JobManagerActor.RestartExistingJobs

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobInfosByContextId])
      daoProbe.reply(JobDAOActor.JobInfos(Seq(jobInfo)))

      daoProbe.expectMsgClass(classOf[JobDAOActor.GetJobConfig])

      daoProbe.expectMsgPF(5.seconds.dilated, "store error in DAO since config not found") {
        case jobInfo: JobDAOActor.SaveJobInfo =>
          jobInfo.jobInfo.state should be(JobStatus.Error)
      }

      managerWatcher.expectTerminated(manager)
    }
  }

  describe("Supervise mode scenarios") {
    it("should restart if running job was found with valid config") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMsg()
    }

    it("should restart if a job was found with valid config and state Restarting") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Restarting, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMsg()
    }

    it("should restart multiple jobs if available for a specific context") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val daoProbe = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId1", state = JobStatus.Restarting))
      dao.saveJobConfig("jobId1", stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsg("StartJob Received")

      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectMsgClass(classOf[JobStarted])
      spyProbe.expectNoMsg()
    }

    it("should restart jobs only if they have status Running or Restarting") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val contextName = "context-name"
      val daoProbe = TestProbe()
      uploadTestJar("test-jar")
      val binInfo = dao.getLastUploadTimeAndType("test-jar")
      val binaryInfo = BinaryInfo("test-jar", binInfo.get._2, binInfo.get._1)
      val jobInfo = JobInfo("jobId", contextId, contextName,
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId,context.name=$contextName").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId1", state = JobStatus.Error))
      dao.saveJobConfig("jobId1", stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId2", state = JobStatus.Finished))
      dao.saveJobConfig("jobId2", stringConfig)
      val jobInfo3 = jobInfo.copy(jobId = "jobId3", state = JobStatus.Restarting)
      dao.saveJobInfo(jobInfo3)
      dao.saveJobConfig("jobId3", stringConfig)

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsg("StartJob Received")

      spyProbe.expectMsgAllOf(JobStarted(jobInfo.jobId, jobInfo),
          JobStarted(jobInfo3.jobId, jobInfo3.copy(state = JobStatus.Running)))
      spyProbe.expectNoMsg()
    }

    it("should keep the context JVM running if restart of atleast 1 job is successful") {
      val spyProbe = TestProbe()
      val contextId = "dummy-context"
      val deathWatcher = TestProbe()
      val daoProbe = TestProbe()
      val binaryInfo = BinaryInfo("demo", BinaryType.Jar, DateTime.now())
      val jobInfo = JobInfo("jobId", contextId, "context-name",
          binaryInfo, wordCountClass, JobStatus.Running, DateTime.now(), None, None)
      manager = system.actorOf(JobManagerActorSpy.props(daoActor, "", 5.seconds, contextId, spyProbe))
      deathWatcher.watch(manager)

      contextConfig = ConfigFactory.parseString(s"context.id=$contextId").withFallback(contextConfig)
      manager ! JobManagerActor.Initialize(contextConfig, None, emptyActor)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      dao.saveJobInfo(jobInfo)
      dao.saveJobConfig(jobInfo.jobId, stringConfig)
      dao.saveJobInfo(jobInfo.copy(jobId = "jobId1"))

      manager ! JobManagerActor.RestartExistingJobs

      spyProbe.expectMsg("StartJob Received")
      spyProbe.expectMsgAllClassOf(classOf[JobStarted], classOf[JobRestartFailed])
      spyProbe.expectNoMsg()
      deathWatcher.expectNoMsg()
    }
  }
}
