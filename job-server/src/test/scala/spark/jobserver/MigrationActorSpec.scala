package spark.jobserver

import java.io.File

import com.typesafe.config.ConfigFactory
import spark.jobserver.io.BinaryDAO
import spark.jobserver.util.{HDFSClusterLike, MigrationMetaDataSpec}
import akka.actor.ActorRef
import akka.testkit.TestProbe
import org.apache.commons.io.FileUtils
import spark.jobserver.MigrationActor._

import scala.concurrent.duration._
import collection.JavaConverters._
import scala.io.Source

object MigrationActorSpec extends JobSpecConfig

class MigrationActorSpec extends JobSpecBase(MigrationActorSpec.getNewSystem) with HDFSClusterLike {
  private var migrationActor: ActorRef = _
  var daoActorProb: TestProbe = _

  override def beforeAll: Unit = {
    super.startHDFS()
  }

  before {
    daoActorProb = TestProbe()
    val testClusterUrl = getNameNodeURI()
    val config = Map(
      "spark.jobserver.combineddao.binarydao.dir" -> s"$testClusterUrl/migration-test",
      "spark.jobserver.sqldao.rootdir" -> "/tmp"
    ).asJava

    migrationActor = system.actorOf(
      MigrationActor.props(
        ConfigFactory.parseMap(config),
        daoActorProb.ref,
        initRetry = 1.seconds,
        syncInterval = 1.seconds,
        autoStartSync = false))
  }

  after {
    // Cleanup sync file from /tmp folder
    FileUtils.deleteQuietly(new File(MigrationMetaDataSpec.TEST_META_DATA_FILE))
    super.cleanHDFS("/migration-test/")
  }

  override def afterAll(): Unit = {
    super.shutdownHDFS()
  }

  describe("Live Request Tests") {
    it("should store binary to HDFS") {
      migrationActor ! MigrationActor.SaveBinaryInHDFS("dummy", Array(1, 2))
      val hash = BinaryDAO.calculateBinaryHashString(Array(1, 2))

      Thread.sleep(2000)

      super.fileExists(s"/migration-test/$hash") should be(true)
    }

    it("should not give error if same binary is saved again") {
      migrationActor ! MigrationActor.SaveBinaryInHDFS("dummy", Array(1, 2))
      migrationActor ! MigrationActor.SaveBinaryInHDFS("dummy", Array(1, 2))
      val hash = BinaryDAO.calculateBinaryHashString(Array(1, 2))

      Thread.sleep(2000)

      super.readFile(s"$getNameNodeURI/migration-test/$hash") should be(Array(1, 2))
    }

    it("should delete a binary from hdfs if exists") {
      // Save binary and verify it exists
      migrationActor ! MigrationActor.SaveBinaryInHDFS("dummy", Array(1, 2))
      val hash = BinaryDAO.calculateBinaryHashString(Array(1, 2))
      Thread.sleep(2000)
      super.readFile(s"$getNameNodeURI/migration-test/$hash") should be(Array(1, 2))

      // Remove binary
      migrationActor ! MigrationActor.DeleteBinaryFromHDFS("dummy")
      daoActorProb.expectMsg(GetHashForApp("dummy"))
      daoActorProb.reply(GetHashForAppSucceeded(Seq(hash)))
      super.fileExists(s"$getNameNodeURI/migration-test/$hash") should be(false)
    }
  }

  describe("Sync Request Tests - Good cases") {
    it("should get all hashes and store them in local disk") {
      migrationActor ! MigrationActor.Init
      daoActorProb.expectMsg(GetAllHashes)
      daoActorProb.reply(GetAllHashesSucceeded(Seq("hash_a", "hash_b")))

      Thread.sleep(2000)

      val readFile = Source.fromFile(MigrationMetaDataSpec.TEST_META_DATA_FILE).getLines().toList
      readFile(0) should be(s"${MigrationMetaData.CURRENT_KEY} = 1")
      readFile(1) should be(s"${MigrationMetaData.TOTAL_KEYS} = 2")
      readFile(2) should be(s"1 = hash_a")
      readFile(3) should be(s"2 = hash_b")

      daoActorProb.expectMsg(2.seconds, GetBinary("hash_a")) // Verify schedular has started
    }

    it("should retrieve and store binaries one after another") {
      migrationActor ! MigrationActor.Init

      // Return hashes
      daoActorProb.expectMsg(2.seconds, GetAllHashes)
      daoActorProb.reply(GetAllHashesSucceeded(Seq("hash_a", "hash_b")))

      // Return first hash
      daoActorProb.expectMsg(2.seconds, GetBinary("hash_a"))
      daoActorProb.reply(GetBinarySucceeded(Array(1, 2)))

      // Return second hash
      daoActorProb.expectMsg(2.seconds, GetBinary("hash_b"))
      daoActorProb.reply(GetBinarySucceeded(Array(3, 4)))

      daoActorProb.expectNoMsg(2.seconds) // Verify that scheduler has stopped

      Thread.sleep(2000)

      // Verify files in HDFS
      super.readFile(s"$getNameNodeURI/migration-test/hash_a") should be(Array(1, 2))
      super.readFile(s"$getNameNodeURI/migration-test/hash_b") should be(Array(3, 4))
    }
  }

  describe("Sync Request Tests - Failure cases") {
    it("should retry if init fails") {
      migrationActor ! MigrationActor.Init

      // Fail hashes fetch
      daoActorProb.expectMsg(2.seconds, GetAllHashes)
      daoActorProb.reply(GetAllHashesFailed)

      // Fail hashes fetch
      daoActorProb.expectMsg(2.seconds, GetAllHashes)
      daoActorProb.reply(GetAllHashesFailed)

      // Pass hashes fetch
      daoActorProb.expectMsg(2.seconds, GetAllHashes)
      daoActorProb.reply(GetAllHashesSucceeded(Seq()))

      daoActorProb.expectNoMsg(2.seconds)
    }

    it("should retry fetching binary if it fails") {
      migrationActor ! MigrationActor.Init

      // Return hashes
      daoActorProb.expectMsg(2.seconds, GetAllHashes)
      daoActorProb.reply(GetAllHashesSucceeded(Seq("hash_a", "hash_b")))

      // Fail fetch request
      daoActorProb.expectMsg(2.seconds, GetBinary("hash_a"))
      daoActorProb.reply(GetBinaryFailed)

      // Fail fetch request by exception
      daoActorProb.expectMsg(2.seconds, GetBinary("hash_a"))
      daoActorProb.reply(new Exception("Thou shall fail!"))

      daoActorProb.expectMsg(2.seconds, GetBinary("hash_a"))
      daoActorProb.reply(GetBinarySucceeded(Array(1, 2)))

      daoActorProb.expectMsg(2.seconds, GetBinary("hash_b"))
      daoActorProb.reply(GetBinarySucceeded(Array(3, 4)))

      daoActorProb.expectNoMsg(2.seconds) // Verify that scheduler has stopped

      // Verify files in HDFS
      super.readFile(s"$getNameNodeURI/migration-test/hash_a") should be(Array(1, 2))
      super.readFile(s"$getNameNodeURI/migration-test/hash_b") should be(Array(3, 4))
    }

    it("should retry saving binary if save fails due to HDFS errors") {
      migrationActor ! MigrationActor.Init

      // Return hashes
      daoActorProb.expectMsg(2.seconds, GetAllHashes)
      daoActorProb.reply(GetAllHashesSucceeded(Seq("hash_a", "hash_b")))

      super.shutdownHDFS()
      daoActorProb.expectMsg(12.seconds, GetBinary("hash_a"))
      daoActorProb.reply(GetBinarySucceeded(Array(1, 2)))

      daoActorProb.expectMsg(12.seconds, GetBinary("hash_a"))
      daoActorProb.reply(GetBinarySucceeded(Array(1, 2)))

      super.startHDFS()

      daoActorProb.expectMsg(12.seconds, GetBinary("hash_a"))
      daoActorProb.reply(GetBinarySucceeded(Array(1, 2)))

      daoActorProb.expectMsg(12.seconds, GetBinary("hash_b"))
      daoActorProb.reply(GetBinarySucceeded(Array(3, 4)))

      daoActorProb.expectNoMsg(2.seconds) // Verify that scheduler has stopped

      // Verify files in HDFS
      super.readFile(s"$getNameNodeURI/migration-test/hash_a") should be(Array(1, 2))
      super.readFile(s"$getNameNodeURI/migration-test/hash_b") should be(Array(3, 4))
    }

    it("should not give error, if binary is already deleted") {
      migrationActor ! MigrationActor.Init

      // Return hashes
      daoActorProb.expectMsg(2.seconds, GetAllHashes)
      daoActorProb.reply(GetAllHashesSucceeded(Seq("hash_a", "hash_b")))

      daoActorProb.expectMsg(2.seconds, GetBinary("hash_a"))
      daoActorProb.reply(GetBinarySucceeded(Array.emptyByteArray))

      daoActorProb.expectMsg(2.seconds, GetBinary("hash_b"))
      daoActorProb.reply(GetBinarySucceeded(Array(3, 4)))

      daoActorProb.expectNoMsg(2.seconds) // Verify that scheduler has stopped

      // Verify files in HDFS
      super.fileExists("migration-test/hash_a") should be(false)
      super.readFile(s"$getNameNodeURI/migration-test/hash_b") should be(Array(3, 4))
    }

    it("should not fetch hashes again if sync file already exists locally") {
      migrationActor ! MigrationActor.Init
      daoActorProb.expectMsg(GetAllHashes)
      daoActorProb.reply(GetAllHashesSucceeded(Seq("hash_a", "hash_b")))

      Thread.sleep(2000)

      migrationActor ! MigrationActor.Init
      daoActorProb.expectMsg(2.seconds, GetBinary("hash_a"))
    }
  }
}
