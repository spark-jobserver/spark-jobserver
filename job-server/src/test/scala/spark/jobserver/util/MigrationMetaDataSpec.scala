package spark.jobserver.util

import java.io.File
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import spark.jobserver.MigrationMetaData
import scala.io.Source

object MigrationMetaDataSpec {
  val TEST_META_DATA_FILE = s"/tmp/${MigrationMetaData.FILE_NAME}"
}

class MigrationMetaDataSpec extends FunSpec with Matchers with BeforeAndAfter {
  var metadata: MigrationMetaData = _

  before {
    metadata = new MigrationMetaData("/tmp")
  }

  after {
    FileUtils.forceDelete(new File(MigrationMetaDataSpec.TEST_META_DATA_FILE))
  }

  describe("Migration metadata tests") {
    it("should store passed data to file") {
      metadata.save(Seq("a", "b", "c"))

      val readFile = Source.fromFile(MigrationMetaDataSpec.TEST_META_DATA_FILE).getLines().toList
      readFile(0) should be(s"${MigrationMetaData.CURRENT_KEY} = 1")
      readFile(1) should be(s"${MigrationMetaData.TOTAL_KEYS} = 3")
      readFile(2) should be(s"1 = a")
      readFile(3) should be(s"2 = b")
      readFile(4) should be(s"3 = c")
    }

    it("should return values one by one") {
      metadata.save(Seq("a", "b", "c"))

      metadata.getNext() should be(Some("a"))
      metadata.updateIndex()
      metadata.getNext() should be(Some("b"))
      metadata.updateIndex()
      metadata.getNext() should be(Some("c"))
      metadata.updateIndex()
      metadata.getNext() should be(None)
      metadata.getNext() should be(None)

      val readFile = Source.fromFile(MigrationMetaDataSpec.TEST_META_DATA_FILE).getLines().toList
      readFile(0) should be(s"${MigrationMetaData.CURRENT_KEY} = 4")
      readFile(1) should be(s"${MigrationMetaData.TOTAL_KEYS} = 3")

      metadata.allConsumed should be(true)
    }

    it("should restart from the last point") {
      metadata.save(Seq("a", "b", "c"))

      // Update the config with index 2, simulating that some values were read
      val config = new PropertiesConfiguration(MigrationMetaDataSpec.TEST_META_DATA_FILE)
      config.setProperty(MigrationMetaData.CURRENT_KEY, "2")
      config.save()

      // Simulate reloading of config after sjs restart
      metadata = new MigrationMetaData("/tmp")

      metadata.getNext() should be (Some("b"))
      metadata.allConsumed should be (false)
      metadata.updateIndex()

      metadata.getNext() should be (Some("c"))
      metadata.allConsumed should be (false)
      metadata.updateIndex()

      metadata.getNext() should be (None)
      metadata.allConsumed should be (true)
    }
  }
}


