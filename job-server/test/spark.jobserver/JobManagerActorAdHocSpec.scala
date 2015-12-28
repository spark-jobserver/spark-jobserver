package spark.jobserver

import akka.testkit.TestProbe
import spark.jobserver.io.JobDAOActor

/**
 * This tests JobManagerActor of AdHoc context.
 */
class JobManagerActorAdHocSpec extends JobManagerSpec {

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(JobManagerSpec.getContextConfig(adhoc = true)))
    supervisor = TestProbe().ref
  }

}
