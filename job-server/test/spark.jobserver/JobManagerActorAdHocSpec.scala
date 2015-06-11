package spark.jobserver

import akka.testkit.TestProbe
import spark.jobserver.io.JobDAOActor

/**
 * This tests JobManagerActor of AdHoc context.
 */
class JobManagerActorAdHocSpec extends JobManagerSpec(adhoc = true) {

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props())
    supervisor = TestProbe().ref
  }

}
