package spark.jobserver.io

import java.util.concurrent.TimeUnit
import org.scalatest.BeforeAndAfter

import java.time.ZonedDateTime
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class InMemoryMetaDAOSpec extends AnyFunSpecLike with Matchers with BeforeAndAfter {

  private var inMemoryMetaDao: InMemoryMetaDAO = _

  before {
    inMemoryMetaDao = new InMemoryMetaDAO
  }

  describe("Get binaries tests") {
    it("should return latest binaries for each name group if uploaded with same name") {
      val current = ZonedDateTime.now()

      val binariesFuture = for {
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current, "")
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current.plusMinutes(2), "")
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current.plusMinutes(1), "")
        _ <- inMemoryMetaDao.saveBinary("test1", BinaryType.Jar, current, "")
        binaries <- inMemoryMetaDao.getBinaries
      } yield binaries

      val binaries = Await.result(binariesFuture, Duration(3, TimeUnit.SECONDS))

      val expectedElements = Seq(BinaryInfo("test", BinaryType.Jar, current.plusMinutes(2), Some("")),
                                 BinaryInfo("test1", BinaryType.Jar, current, Some("")))
      binaries should contain allElementsOf(expectedElements)
    }

    it("should get the latest one if multiple binaries are uploaded with the same name") {
      val current = ZonedDateTime.now()

      val binariesFuture = for {
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current, "")
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current.plusMinutes(2), "")
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current.plusMinutes(1), "")
        _ <- inMemoryMetaDao.saveBinary("test1", BinaryType.Jar, current, "")
        binaries <- inMemoryMetaDao.getBinary("test")
      } yield binaries

      val binaries = Await.result(binariesFuture, Duration(3, TimeUnit.SECONDS))

      val expectedBinary = BinaryInfo("test", BinaryType.Jar, current.plusMinutes(2), Some(""))
      binaries.get should be(expectedBinary)
    }

    it("should delete binary") {
      val current = ZonedDateTime.now()

      val binariesFuture = for {
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current, "")
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current.plusMinutes(2), "")
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current.plusMinutes(1), "")
        _ <- inMemoryMetaDao.saveBinary("test1", BinaryType.Jar, current, "")
        _ <- inMemoryMetaDao.deleteBinary("test")
        binaries <- inMemoryMetaDao.getBinaries
      } yield binaries

      val binaries = Await.result(binariesFuture, Duration(3, TimeUnit.SECONDS))

      val expectedBinary = BinaryInfo("test1", BinaryType.Jar, current, Some(""))
      binaries(0) should be(expectedBinary)
    }

    it("should get binaries based on storage id") {
      val current = ZonedDateTime.now()

      val binariesFuture = for {
        _ <- inMemoryMetaDao.saveBinary("test", BinaryType.Jar, current, "s1")
        _ <- inMemoryMetaDao.saveBinary("test1", BinaryType.Jar, current, "s2")
        binaries <- inMemoryMetaDao.getBinariesByStorageId("s2")
      } yield binaries

      val binaries = Await.result(binariesFuture, Duration(3, TimeUnit.SECONDS))
      println(binaries)

      val expectedBinary = BinaryInfo("test1", BinaryType.Jar, current, Some("s2"))
      binaries(0) should be(expectedBinary)
    }
  }
}
