package spark.jobserver.util

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.scalatest.{Matchers, FunSpec}

class SparkJobUtilsSpec extends FunSpec with Matchers {
  import collection.JavaConverters._

  val config = ConfigFactory.parseMap(Map(
                 "spark.home" -> "/etc/spark",
                 "spark.master" -> "local[4]"
               ).asJava)
  val contextName = "demo"

  def getSparkConf(configMap: Map[String, Any]): SparkConf =
    SparkJobUtils.configToSparkConf(config, ConfigFactory.parseMap(configMap.asJava), contextName)

  describe("SparkJobUtils.configToSparkConf") {
    it("should translate num-cpu-cores and memory-per-node properly") {
      val sparkConf = getSparkConf(Map("num-cpu-cores" -> 4, "memory-per-node" -> "512m"))
      sparkConf.get("spark.master") should equal ("local[4]")
      sparkConf.get("spark.cores.max") should equal ("4")
      sparkConf.get("spark.executor.memory") should equal ("512m")
      sparkConf.get("spark.home") should equal ("/etc/spark")
    }

    it("should add other arbitrary settings") {
      val sparkConf = getSparkConf(Map("spark.cleaner.ttl" -> 86400))
      sparkConf.getInt("spark.cleaner.ttl", 0) should equal (86400)
    }

    it("should read contextCreationTimeout for standalone mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "local[4]",
        "spark.jobserver.yarn-context-creation-timeout" -> 20000,
        "spark.jobserver.context-creation-timeout" -> 10000
      ).asJava)
      SparkJobUtils.getContextCreationTimeout(config) should equal (10)
    }

    it("should read default contextCreationTimeout for standalone mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "local[4]"
      ).asJava)
      SparkJobUtils.getContextCreationTimeout(config) should equal (15)
    }

    it("should read contextCreationTimeout for yarn-client mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "yarn-client",
        "spark.jobserver.yarn-context-creation-timeout" -> 20000,
        "spark.jobserver.context-creation-timeout" -> 10000
      ).asJava)
      SparkJobUtils.getContextCreationTimeout(config) should equal (20)
    }

    it("should read default contextCreationTimeout for yarn-client mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "yarn-client"
      ).asJava)
      SparkJobUtils.getContextCreationTimeout(config) should equal (40)
    }

    it("should read contextDeletionTimeout for standalone mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "local[4]",
        "spark.jobserver.yarn-context-deletion-timeout" -> 20000,
        "spark.jobserver.context-deletion-timeout" -> 10000
      ).asJava)
      SparkJobUtils.getContextDeletionTimeout(config) should equal (10)
    }

    it("should read default contextDeletionTimeout for standalone mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "local[4]"
      ).asJava)
      SparkJobUtils.getContextDeletionTimeout(config) should equal (15)
    }

    it("should read contextDeletionTimeout for yarn-client mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "yarn-client",
        "spark.jobserver.yarn-context-deletion-timeout" -> 20000,
        "spark.jobserver.context-deletion-timeout" -> 10000
      ).asJava)
      SparkJobUtils.getContextDeletionTimeout(config) should equal (20)
    }

    it("should read default contextDeletionTimeout for yarn-client mode") {
      val config = ConfigFactory.parseMap(Map(
        "spark.master" -> "yarn-client"
      ).asJava)
      SparkJobUtils.getContextDeletionTimeout(config) should equal (40)
    }
  }
}