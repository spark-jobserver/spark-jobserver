package spark.jobserver

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import akka.testkit.TestProbe
import spark.jobserver.CommonMessages.{ JobErroredOut, JobResult }
import spark.jobserver.io.JobDAOActor
import collection.JavaConversions._

class NamedObjectsJobSpec extends JobSpecBase(JobManagerSpec.getNewSystem) {

  private val emptyConfig = ConfigFactory.parseString("spark.jobserver.named-object-creation-timeout = 60 s")

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(JobManagerSpec.getContextConfig(adhoc = false)))
    supervisor = TestProbe().ref
  }

  val jobName = "spark.jobserver.NamedObjectsTestJob"

  describe("NamedObjects (RDD)") {
    it("should survive from one job to another one") {

      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.CREATE_DF, ConfigValueFactory.fromAnyRef(false))
        .withValue(NamedObjectsTestJobConfig.CREATE_RDD, ConfigValueFactory.fromAnyRef(true)),
        errorEvents ++ syncEvents)
      val JobResult(_, names: Array[String]) = expectMsgClass(classOf[JobResult])
      names should contain("rdd1")

      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.CREATE_DF, ConfigValueFactory.fromAnyRef(false))
        .withValue(NamedObjectsTestJobConfig.CREATE_RDD, ConfigValueFactory.fromAnyRef(false)),
        errorEvents ++ syncEvents)
      val JobResult(_, names2: Array[String]) = expectMsgClass(classOf[JobResult])

      names2 should contain("rdd1")
      names2 should not contain("df1")

      //clean-up
      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.DELETE, ConfigValueFactory.fromIterable(List("rdd1")))
        .withValue(NamedObjectsTestJobConfig.CREATE_RDD, ConfigValueFactory.fromAnyRef(false)),
        errorEvents ++ syncEvents)
      val JobResult(_, names3: Array[String]) = expectMsgClass(classOf[JobResult])

      names3 should not contain("rdd1")
      names3 should not contain("df1")
    }
  }

  describe("NamedObjects (DataFrame)") {
    it("should survive from one job to another one") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.CREATE_DF, ConfigValueFactory.fromAnyRef(true))
        .withValue(NamedObjectsTestJobConfig.CREATE_RDD, ConfigValueFactory.fromAnyRef(false)),
        errorEvents ++ syncEvents)
      val JobResult(_, names: Array[String]) = expectMsgClass(classOf[JobResult])

      names should contain("df1")

      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.CREATE_DF, ConfigValueFactory.fromAnyRef(false))
        .withValue(NamedObjectsTestJobConfig.CREATE_RDD, ConfigValueFactory.fromAnyRef(false)),
        errorEvents ++ syncEvents)
      val JobResult(_, names2: Array[String]) = expectMsgClass(classOf[JobResult])

      names2 should equal(names)
      
      //clean-up
      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.DELETE, 
          ConfigValueFactory.fromIterable(List("df1"))),
        errorEvents ++ syncEvents)
      val JobResult(_, names3: Array[String]) = expectMsgClass(classOf[JobResult])
    }
  }

  describe("NamedObjects (DataFrame + RDD)") {
    it("should survive from one job to another one") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.CREATE_DF, ConfigValueFactory.fromAnyRef(true))
        .withValue(NamedObjectsTestJobConfig.CREATE_RDD, ConfigValueFactory.fromAnyRef(true)),
        errorEvents ++ syncEvents)
      val JobResult(_, names: Array[String]) = expectMsgClass(classOf[JobResult])
      
      names should contain("rdd1")
      names should contain("df1")

      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.CREATE_DF, ConfigValueFactory.fromAnyRef(false))
        .withValue(NamedObjectsTestJobConfig.CREATE_RDD, ConfigValueFactory.fromAnyRef(false)),
        errorEvents ++ syncEvents)
      val JobResult(_, names2: Array[String]) = expectMsgClass(classOf[JobResult])

      names2 should equal(names)
      
      //clean-up
      manager ! JobManagerActor.StartJob("demo", jobName, emptyConfig.withValue(NamedObjectsTestJobConfig.DELETE,
          ConfigValueFactory.fromIterable(List("rdd1", "df1"))),
        errorEvents ++ syncEvents)
      val JobResult(_, names3: Array[String]) = expectMsgClass(classOf[JobResult])
    }
  }

}
