package spark.jobserver

import java.io.File
import java.nio.file.{Files, StandardOpenOption}

import com.typesafe.config.ConfigFactory
import spark.jobserver.CommonMessages.{JobErroredOut, JobResult}
import spark.jobserver.DataManagerActor.RetrieveData
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.JobDAOActor

import scala.concurrent.Await

class JobManagerActorSpec extends JobManagerSpec {
  import scala.concurrent.duration._
  import akka.testkit._

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    contextConfig = JobManagerSpec.getContextConfig(adhoc = false)
    manager = system.actorOf(JobManagerActor.props(daoActor))
    supervisor = TestProbe().ref
  }

  after {
    AkkaTestUtils.shutdownAndWait(manager)
  }

  describe("starting jobs") {
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
}
