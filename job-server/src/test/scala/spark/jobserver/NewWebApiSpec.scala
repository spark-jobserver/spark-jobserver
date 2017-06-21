package spark.jobserver

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import spray.json._

import spark.jobserver.io.{JobDAOActor, JobInfo}
import spark.jobserver.services.{JobServerServices, JobServiceJson}
import spark.jobserver.util.DummyActor

class NewWebApiSpec
    extends FunSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with JobServiceJson with DefaultJsonProtocol
    with ScalaFutures {

  val bindConfKey   = "spark.jobserver.bind-address"
  val bindConfVal   = "0.0.0.0"
  val masterConfKey = "spark.master"
  val masterConfVal = "spark://localhost:7077"
  val config        = ConfigFactory.parseString(s"""
    spark {
      master = "$masterConfVal"
      jobserver.bind-address = "$bindConfVal"
      jobserver.short-timeout = 3 s
    }
    spray.can.server {}
    shiro {
      authentication = off
    }""")

  val dummyPort   = 9999
  val jobDaoActor = system.actorOf(JobDAOActor.props(new InMemoryDAO))
  val statusActor = system.actorOf(JobStatusActor.props(jobDaoActor))
  val dummyActor  = system.actorOf(Props(classOf[DummyActor], statusActor, config))

  val newapi    = new JobServerServices(config, dummyPort - 1, dummyActor, dummyActor, dummyActor, dummyActor)
  val newroutes = newapi.route

  describe("list jobs") {
    it("should list jobs correctly") {
      Get("/jobs") ~> newroutes ~> check {
        status shouldEqual StatusCodes.OK
        println(responseAs[String])
        /* should be(
          Seq(
            Map(
              "jobId"     -> "foo-1",
              "startTime" -> "2013-05-29T00:00:00.000Z",
              "classPath" -> "com.abc.meme",
              "context"   -> "context",
              "duration"  -> "Job not done yet",
              StatusKey   -> JobStatus.Running
            ),
            Map(
              "jobId"     -> "foo-1",
              "startTime" -> "2013-05-29T00:00:00.000Z",
              "classPath" -> "com.abc.meme",
              "context"   -> "context",
              "duration"  -> "300.0 secs",
              StatusKey   -> JobStatus.Finished
            )
          ))
       */
      }
    }
  }
}
