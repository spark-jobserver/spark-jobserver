package spark.jobserver

import com.typesafe.config.ConfigFactory
import spark.jobserver.JobManagerActor.KillJob
import scala.collection.mutable
import spark.jobserver.io.JobDAO

object JobManagerSpec extends JobSpecConfig

abstract class JobManagerSpec(adhoc: Boolean) extends JobSpecBase(JobManagerSpec.getNewSystem) {
  import scala.concurrent.duration._
  import CommonMessages._
  import JobManagerSpec.MaxJobsPerContext
  import akka.testkit._

  val classPrefix = "spark.jobserver."
  private val wordCountClass = classPrefix + "WordCountExample"
  protected val stringConfig = ConfigFactory.parseString("input.string = The lazy dog jumped over the fish")
  protected val emptyConfig = ConfigFactory.parseString("spark.master = bar")

  describe("error conditions") {
    it("should return errors if appName does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo2", wordCountClass, emptyConfig, Set.empty[Class[_]])
      expectMsg(CommonMessages.NoSuchApplication)
    }

    it("should return error message if classPath does not match") {
      uploadTestJar()
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("demo", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(CommonMessages.NoSuchClass)
    }

    it("should error out if loading garbage jar") {
      uploadJar(dao, "../README.md", "notajar")
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])
      manager ! JobManagerActor.StartJob("notajar", "no.such.class", emptyConfig, Set.empty[Class[_]])
      expectMsg(CommonMessages.NoSuchClass)
    }

    it("should error out if job validation fails") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, emptyConfig, allEvents)
      expectMsgClass(classOf[CommonMessages.JobValidationFailed])
      expectNoMsg()
    }
  }

  describe("starting jobs") {
    it("should start job and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job more than one time and return result successfully (all events)") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()

      // should be ok to run the same more again
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, allEvents)
      expectMsgClass(classOf[JobStarted])
      expectMsgAllClassOf(classOf[JobFinished], classOf[JobResult])
      expectNoMsg()
    }

    it("should start job and return results (sync route)") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Map[String, Int]) => println("I got results! " + result)
      }
      expectNoMsg()
    }

    it("should start job and return JobStarted (async)") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", wordCountClass, stringConfig, errorEvents ++ asyncEvents)
      expectMsgClass(classOf[JobStarted])
      expectNoMsg()
    }

    it("should return error if job throws an error") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "MyErrorJob", emptyConfig, errorEvents)
      val errorMsg = expectMsgClass(classOf[JobErroredOut])
      errorMsg.err.getClass should equal (classOf[IllegalArgumentException])
    }

    it("job should get jobConfig passed in to StartJob message") {
      val jobConfig = ConfigFactory.parseString("foo.bar.baz = 3")
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "ConfigCheckerJob", jobConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, keys: Seq[String]) =>
          keys should contain ("foo")
      }
    }

    it("should properly serialize case classes and other job jar classes") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "ZookeeperJob", stringConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Array[Product]) =>
          result.length should equal (1)
          result(0).getClass.getName should include ("Animal")
      }
      expectNoMsg()
    }

    it ("should refuse to start a job when too many jobs in the context are running") {
      val jobSleepTimeMillis = 2000L
      val jobConfig = ConfigFactory.parseString("sleep.time.millis = " + jobSleepTimeMillis)

      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()

      val messageCounts = new mutable.HashMap[Class[_], Int].withDefaultValue(0)
      // Try to start 3 instances of this job. 2 of them should start, and the 3rd should be denied.
      for (i <- 0 until MaxJobsPerContext + 1) {
        manager ! JobManagerActor.StartJob("demo", classPrefix + "SleepJob", jobConfig, allEvents)
      }

      while (messageCounts.values.sum < (MaxJobsPerContext * 3 + 1)) {
        expectMsgPF(3.seconds.dilated, "Expected a message but didn't get one!") {
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
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", JobManagerSpec.config, adhoc, supervisor)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "SimpleObjectJob", emptyConfig,
        syncEvents ++ errorEvents)
      expectMsgPF(3.seconds.dilated, "Did not get JobResult") {
        case JobResult(_, result: Int) => result should equal (1 + 2 + 3)
      }
    }

    it("should be able to cancel running job") {
      manager ! JobManagerActor.Initialize
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "LongPiJob", stringConfig, allEvents)
      expectMsgPF(1.seconds.dilated, "Did not get JobResult") {
        case JobStarted(id, _, _) => {
          manager ! KillJob(id)
          expectMsgClass(classOf[JobKilled])
        }
      }
    }

  }
}
