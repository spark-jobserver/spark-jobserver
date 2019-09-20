package spark.jobserver.io.zookeeper

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpecLike
import org.scalatest.Matchers

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.io.BinaryDAO
import spark.jobserver.io.BinaryInfo
import spark.jobserver.io.BinaryType
import spark.jobserver.io.ContextInfo
import spark.jobserver.io.ErrorData
import spark.jobserver.io.JobDAOActor.ContextInfos
import spark.jobserver.io.JobDAOActor.GetContextInfos
import spark.jobserver.io.JobDAOActor.GetJobInfos
import spark.jobserver.io.JobDAOActor.JobInfos
import spark.jobserver.io.JobInfo
import spark.jobserver.util.CuratorTestCluster
import spark.jobserver.util.Utils

object AutoPurgeActorSpec {
  val system = ActorSystem("test")
}

object DummyZookeeperDAOActor{
  def props(dao : MetaDataZookeeperDAO): Props = Props(classOf[DummyZookeeperDAOActor], dao)
}

class DummyZookeeperDAOActor(dao : MetaDataZookeeperDAO) extends InstrumentedActor {
  val timeout = 60 seconds
  override def wrappedReceive: Receive = {
    case GetContextInfos(_,_) => sender ! ContextInfos(Await.result(dao.getContexts(None, None), timeout))
    case GetJobInfos(_) => sender ! JobInfos(Await.result(dao.getJobs(5000, None), timeout))
  }
}

class AutoPurgeActorSpec extends TestKit(AutoPurgeActorSpec.system) with FunSpecLike with Matchers
  with BeforeAndAfter with ImplicitSender{

  /*
   * Setup
   */

  private val timeout = 60 seconds
  private val testServer = new CuratorTestCluster()

  def config: Config = ConfigFactory.parseString(
    s"""
         |spark.jobserver.zookeeperdao.connection-string = "${testServer.getConnectString}"
         |spark.jobserver.combineddao.binarydao.class = spark.jobserver.io.HdfsBinaryDAO
         |spark.jobserver.combineddao.metadatadao.class = spark.jobserver.io.zookeeper.MetaDataZookeeperDAO
         |spark.jobserver.zookeeperdao.autopurge = true
         |spark.jobserver.zookeeperdao.autopurge_after_hours = 168
    """.stripMargin
  ).withFallback(
    ConfigFactory.load("local.test.combineddao.conf")
  )

  var purgeActor : ActorRef = _
  val dao = new MetaDataZookeeperDAO(config)
  val zkUtils = new ZookeeperUtils(config)
  val daoActor = system.actorOf(DummyZookeeperDAOActor.props(dao))

  before {
    // Empty database
    Utils.usingResource(zkUtils.getClient) {
      client =>
        zkUtils.delete(client, "")
    }
    // Create actor
    purgeActor = system.actorOf(AutoPurgeActor.props(config, daoActor, 8 * 24))
  }

  /*
   * Test data
   */

  val age = 8 * 24 * 60 * 60 * 1000
  val rightNow = new DateTime()
  val backThen = new DateTime(rightNow.getMillis - age)
  val bin = BinaryInfo("binaryWithJar", BinaryType.Jar, backThen,
    Some(BinaryDAO.calculateBinaryHashString("1".getBytes)))
  val oldContext = ContextInfo("1", "someName", "someConfig", Some("ActorAddress"), backThen,
      Some(backThen), "FINISHED", None)
  val recentContext = oldContext.copy(id = "2", endTime = Some(rightNow))
  val runningContext = oldContext.copy(id = "3", state = "RUNNING")
  val oldJob = JobInfo("1", "someContextId", "someContextName", "someClassPath", "FINISHED",
      backThen, Some(backThen), Some(ErrorData("someMessage", "someError", "someTrace")), Seq(bin))
  val recentJob = oldJob.copy(jobId = "2", endTime = Some(rightNow))
  val runningJob = oldJob.copy(jobId = "3", state = "RUNNING")

  /*
   * Tests
   */

  it("Should accept a correct configuration"){
    AutoPurgeActor.isEnabled(config) should equal(true)
  }

  it("Should purge old contexts and jobs"){
    Await.result(dao.saveBinary(bin.appName, bin.binaryType, bin.uploadTime, bin.binaryStorageId.get),
      timeout) should equal(true)
    Await.result(dao.saveContext(oldContext), timeout) should equal(true)
    Await.result(dao.saveJob(oldJob), timeout) should equal(true)
    Await.result(dao.getContexts(None, None), timeout).size should equal(1)
    Await.result(dao.getJobs(100, None), timeout).size should equal(1)

    purgeActor ! AutoPurgeActor.PurgeOldData

    expectMsg(timeout, AutoPurgeActor.PurgeComplete)
    Await.result(dao.getJobs(100, None), timeout).size should equal(0)
    Await.result(dao.getContexts(None, None), timeout).size should equal(0)
  }

  it("Should not purge recent contexts and jobs"){
    Await.result(dao.saveBinary(bin.appName, bin.binaryType, bin.uploadTime, bin.binaryStorageId.get),
      timeout) should equal(true)
    Await.result(dao.saveContext(oldContext), timeout) should equal(true)
    Await.result(dao.saveContext(recentContext), timeout) should equal(true)
    Await.result(dao.saveJob(oldJob), timeout) should equal(true)
    Await.result(dao.saveJob(recentJob), timeout) should equal(true)
    Await.result(dao.getContexts(None, None), timeout).size should equal(2)
    Await.result(dao.getJobs(100, None), timeout).size should equal(2)

    purgeActor ! AutoPurgeActor.PurgeOldData

    expectMsg(timeout, AutoPurgeActor.PurgeComplete)
    Await.result(dao.getJobs(100, None), timeout) should equal(Seq(recentJob))
    Await.result(dao.getContexts(None, None), timeout) should equal(Seq(recentContext))
  }

  it("Should only purge contexts and jobs in a final state"){
    Await.result(dao.saveBinary(bin.appName, bin.binaryType, bin.uploadTime, bin.binaryStorageId.get),
      timeout) should equal(true)
    Await.result(dao.saveContext(oldContext), timeout) should equal(true)
    Await.result(dao.saveContext(runningContext), timeout) should equal(true)
    Await.result(dao.saveJob(oldJob), timeout) should equal(true)
    Await.result(dao.saveJob(runningJob), timeout) should equal(true)
    Await.result(dao.getContexts(None, None), timeout).size should equal(2)
    Await.result(dao.getJobs(100, None), timeout).size should equal(2)

    purgeActor ! AutoPurgeActor.PurgeOldData

    expectMsg(timeout, AutoPurgeActor.PurgeComplete)
    Await.result(dao.getJobs(100, None), timeout) should equal(Seq(runningJob))
    Await.result(dao.getContexts(None, None), timeout) should equal(Seq(runningContext))
  }

}