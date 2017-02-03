package spark.jobserver

import spark.jobserver.io.BinaryType

import scala.collection.mutable

import com.typesafe.config.ConfigFactory
import spark.jobserver.JobManagerActor.KillJob

object JobManagerSpec extends JobSpecConfig

abstract class JobManagerSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {
  import scala.concurrent.duration._

  import CommonMessages._
  import JobManagerSpec.MaxJobsPerContext
  import akka.testkit._

  val classPrefix = "spark.jobserver."
  private val wordCountClass = classPrefix + "WordCountExample"
  private val newWordCountClass = classPrefix + "WordCountExampleNewApi"
  private val javaJob = classPrefix + "JavaHelloWorldJob"
  val sentence = "The lazy dog jumped over the fish"
  val counts = sentence.split(" ").groupBy(x => x).mapValues(_.length)
  protected val stringConfig = ConfigFactory.parseString(s"input.string = $sentence")
  protected val emptyConfig = ConfigFactory.parseString("spark.master = bar")

  val initMsgWait = 10.seconds.dilated
  val startJobWait = 5.seconds.dilated

  describe("error conditions") {
    it("should return errors if appName does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo2", wordCountClass, emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchApplication)
    }

    it("should return error message if classPath does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should error out if loading garbage jar") {
      uploadBinary(dao, "../README.md", "notajar", BinaryType.Jar)
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("notajar", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(startJobWait, CommonMessages.NoSuchClass)
    }

    it("should error out if job validation fails") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, emptyConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMsg()
    }

    it("should error out if new API job validation fails") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", newWordCountClass, emptyConfig, allEvents)
      expectMsgClass(startJobWait, classOf[CommonMessages.JobValidationFailed])
      expectNoMsg()
    }
  }

  describe("starting jobs") {
    it("should start job and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job and return result before job finish event") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectMsgClass(classOf[JobResult])
      expectMsgClass(classOf[JobFinished])
      expectNoMsg()
    }

    it("should start job more than one time and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(None)
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
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMsg()
    }

    it("should start NewAPI job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", newWordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(startJobWait, "Did not get JobResult") {
        case JobResult(_, result) => result should equal (counts)
      }
      expectNoMsg()
    }

    it("should start job and return JobStarted (async)") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, errorEvents ++ asyncEvents)
      expectMsgClass(startJobWait, classOf[JobStarted])
      expectNoMsg()
    }

    it("should return error if job throws an error") {
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "MyErrorJob", emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(startJobWait, classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[RuntimeException])
    }

    it("job should get jobConfig passed in to StartJob message") {
      val jobConfig = ConfigFactory.parseString("foo.bar.baz = 3")
      manager ! JobManagerActor.Initialize(None)
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
      manager ! JobManagerActor.Initialize(None)
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

      manager ! JobManagerActor.Initialize(None)
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
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(initMsgWait, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(5.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Int) => result should equal (1 + 2 + 3)
      }
    }

    it("should be able to cancel running job") {
      manager ! JobManagerActor.Initialize(None)
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
      manager ! JobManagerActor.Initialize(None)
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

  }
}
