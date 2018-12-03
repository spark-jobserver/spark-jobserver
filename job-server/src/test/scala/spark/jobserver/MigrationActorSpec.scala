package spark.jobserver

import java.io.{File, PrintWriter}

import com.typesafe.config.ConfigFactory
import spark.jobserver.io.BinaryDAO
import spark.jobserver.util.{HDFSClusterLike, MigrationMetaDataSpec}
import akka.actor.ActorRef
import akka.actor.Status.Success
import akka.testkit.TestProbe
import org.apache.commons.io.FileUtils
import spark.jobserver.MigrationActor._

import scala.concurrent.duration._
import collection.JavaConverters._
import scala.io.Source
import scala.util.Try

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

  describe("Live Request Tests - Good cases") {
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
      migrationActor ! MigrationActor.GetHashForApp("dummy")
      daoActorProb.expectMsg(GetHashForApp("dummy"))
      daoActorProb.reply(GetHashForAppSucceeded(Seq(hash)))
      expectMsg(2.seconds, GetHashForAppSucceeded(Seq(hash)))

      migrationActor ! MigrationActor.DeleteBinaryFromHDFS(Try(GetHashForAppSucceeded(Seq(hash))))
      expectMsg(2.seconds, "Proceed")
      super.fileExists(s"$getNameNodeURI/migration-test/$hash") should be(false)
    }

    it("should delete all unused hashes against an app from HDFS") {
      // Save binary and verify it exists
      migrationActor ! MigrationActor.SaveBinaryInHDFS("dummyA", Array(1, 2))
      val hashA = BinaryDAO.calculateBinaryHashString(Array(1, 2))
      Thread.sleep(2000)
      super.readFile(s"$getNameNodeURI/migration-test/$hashA") should be(Array(1, 2))

      // Save binary and verify it exists
      migrationActor ! MigrationActor.SaveBinaryInHDFS("dummyA", Array(3, 4))
      val hashB = BinaryDAO.calculateBinaryHashString(Array(3, 4))
      Thread.sleep(2000)
      super.readFile(s"$getNameNodeURI/migration-test/$hashB") should be(Array(3, 4))

      // Get hashes for app name "dummyA"
      migrationActor ! MigrationActor.GetHashForApp("dummyA")
      daoActorProb.expectMsg(GetHashForApp("dummyA"))
      daoActorProb.reply(GetHashForAppSucceeded(Seq(hashA, hashB)))

      // WebApi receives or in this case, this class receives all the hashes
      expectMsg(2.seconds, GetHashForAppSucceeded(Seq(hashA, hashB)))

      // The response is sent again to Migration actor (H2 has finished deleting)
      migrationActor ! MigrationActor.DeleteBinaryFromHDFS(Try(GetHashForAppSucceeded(Seq(hashA, hashB))))
      expectMsg(2.seconds, "Proceed")

      super.fileExists(s"$getNameNodeURI/migration-test/$hashA") should be(false)
      super.fileExists(s"$getNameNodeURI/migration-test/$hashB") should be(false)
    }
  }

  describe("Live Request Tests - Failure cases") {
    it("Should not do anything if no hashes are passed for deletion") {
      migrationActor ! MigrationActor.DeleteBinaryFromHDFS(Try(GetHashForAppSucceeded(Seq())))
      expectMsg(2.seconds, "Empty Seq")
    }

    it("should increment failure counter if even 1 binary failed to delete") {
      // Save binary and verify it exists
      migrationActor ! MigrationActor.SaveBinaryInHDFS("dummyA", Array(1, 2))
      val hashA = BinaryDAO.calculateBinaryHashString(Array(1, 2))
      Thread.sleep(2000)
      super.readFile(s"$getNameNodeURI/migration-test/$hashA") should be(Array(1, 2))

      migrationActor ! MigrationActor.DeleteBinaryFromHDFS(
        Try(GetHashForAppSucceeded(Seq(hashA, "wrong-hash"))))
      expectMsg(2.seconds, "Failed to delete binary")

      super.fileExists(s"$getNameNodeURI/migration-test/$hashA") should be(false)
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

    it("should retrigger sync if already finished") {
      // Create finished sync file
      val syncFileContent =
        s"""|${MigrationMetaData.CURRENT_KEY} = 3
          |${MigrationMetaData.TOTAL_KEYS} = 2
          |1 = hash_a
          |2 = hash_b""".stripMargin
      new PrintWriter(MigrationMetaDataSpec.TEST_META_DATA_FILE) { write(syncFileContent); close }

      migrationActor ! MigrationActor.Init
      daoActorProb.expectMsg(2.seconds, GetAllHashes)

      // Verify that sync file is deleted
      new File(MigrationMetaDataSpec.TEST_META_DATA_FILE).exists() should be(false)
      daoActorProb.reply(GetAllHashesSucceeded(Seq("new_hash_a", "new_hash_b", "new_hash_c")))

      Thread.sleep(2000)

      val readFile = Source.fromFile(MigrationMetaDataSpec.TEST_META_DATA_FILE).getLines().toList
      readFile(0) should be(s"${MigrationMetaData.CURRENT_KEY} = 1")
      readFile(1) should be(s"${MigrationMetaData.TOTAL_KEYS} = 3")
      readFile(2) should be(s"1 = new_hash_a")
      readFile(3) should be(s"2 = new_hash_b")
      readFile(4) should be(s"3 = new_hash_c")

      daoActorProb.expectMsg(2.seconds, GetBinary("new_hash_a")) // Verify schedular has started
    }

    it("should not retrigger if sync is already in progress") {
      // Create sync file
      val syncFileContent =
        s"""|${MigrationMetaData.CURRENT_KEY} = 2
            |${MigrationMetaData.TOTAL_KEYS} = 3
            |1 = hash_a
            |2 = hash_b
            |3 = hash_c""".stripMargin
      new PrintWriter(MigrationMetaDataSpec.TEST_META_DATA_FILE) { write(syncFileContent); close }
      new File(MigrationMetaDataSpec.TEST_META_DATA_FILE).exists() should be(true)

      // Mimic restart of jobserver
      val config = Map(
        "spark.jobserver.combineddao.binarydao.dir" -> s"dummy/migration-test",
        "spark.jobserver.sqldao.rootdir" -> "/tmp"
      ).asJava

      migrationActor = system.actorOf(
        MigrationActor.props(
          ConfigFactory.parseMap(config),
          daoActorProb.ref,
          initRetry = 1.seconds,
          syncInterval = 1.seconds,
          autoStartSync = false))

      migrationActor ! MigrationActor.Init
      daoActorProb.expectMsg(2.seconds, GetBinary("hash_b")) // Should continue from last point
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
