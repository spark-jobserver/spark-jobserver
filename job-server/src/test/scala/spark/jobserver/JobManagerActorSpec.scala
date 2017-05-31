package spark.jobserver

import spark.jobserver.CommonMessages.{JobErroredOut, JobResult}
import spark.jobserver.common.akka.AkkaTestUtils
import spark.jobserver.io.JobDAOActor

class JobManagerActorSpec extends JobManagerSpec {
  import scala.concurrent.duration._
  import akka.testkit._

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(JobManagerSpec.getContextConfig(adhoc = false), daoActor))
    supervisor = TestProbe().ref
  }

  after {
    AkkaTestUtils.shutdownAndWait(manager)
  }
  
  describe("starting jobs") {
    it("jobs should be able to cache RDDs and retrieve them through getPersistentRDDs") {
      manager ! JobManagerActor.Initialize(None)
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
      manager ! JobManagerActor.Initialize(None)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", classPrefix + "CacheRddByNameJob", emptyConfig,
        errorEvents ++ syncEvents)
      expectMsgPF(1.second.dilated, "Expected a JobResult or JobErroredOut message!") {
        case JobResult(_, sum: Int) => sum should equal (1 + 4 + 9 + 16 + 25)
        case JobErroredOut(_, _, error: Throwable) => throw error
      }
    }
  }
}
