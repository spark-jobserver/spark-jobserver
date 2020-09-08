import sbt._
import Versions._
import ExclusionRules._

object Dependencies {

  lazy val typeSafeConfigDeps = "com.typesafe" % "config" % typeSafeConfig

  lazy val yammerDeps = "com.yammer.metrics" % "metrics-core" % metrics

  lazy val miscDeps = Seq(
    "org.scalactic" %% "scalactic" % scalatic,
    "org.mockito" % "mockito-core" % mockito,
    "org.joda" % "joda-convert" % jodaConvert,
    "joda-time" % "joda-time" % jodaTime
  )

  lazy val akkaDeps = Seq(
    // Akka is provided because Spark already includes it, and Spark's version is shaded so it's not safe
    // to use this one
    "com.typesafe.akka" %% "akka-slf4j" % akka,
    "com.typesafe.akka" %% "akka-cluster" % akka exclude("com.typesafe.akka", "akka-remote"),
    "com.typesafe.akka" %% "akka-cluster-tools" % akka,
    "io.spray" %% "spray-json" % sprayJson,
    "io.spray" %% "spray-can" % spray,
    "io.spray" %% "spray-caching" % spray,
    "io.spray" %% "spray-routing-shapeless23" % "1.3.4",
    "io.spray" %% "spray-client" % spray,
    yammerDeps
  )

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % spark % "provided" excludeAll excludeQQ
  )

  lazy val sparkExtraDeps = Seq(
    "org.apache.derby" % "derby" % derby % Provided excludeAll excludeQQ,
    "org.apache.hadoop" % "hadoop-client" % hadoop % Provided excludeAll excludeQQ,
    "org.apache.spark" %% "spark-mllib" % spark % Provided excludeAll excludeQQ,
    "org.apache.spark" %% "spark-sql" % spark % Provided excludeAll excludeQQ,
    "org.apache.spark" %% "spark-streaming" % spark % Provided excludeAll excludeQQ,
    "org.apache.spark" %% "spark-hive" % spark % Provided excludeAll(
      excludeQQ, excludeScalaTest
      )
  )

  lazy val sparkPythonDeps = Seq(
    "net.sf.py4j" % "py4j" % py4j,
    "io.spray" %% "spray-json" % sprayJson % Test
  ) ++ sparkExtraDeps

  lazy val slickDeps = Seq(
    "com.typesafe.slick" %% "slick" % slick,
    "com.h2database" % "h2" % h2,
    "org.postgresql" % "postgresql" % postgres,
    "mysql" % "mysql-connector-java" % mysql,
    "commons-dbcp" % "commons-dbcp" % commons,
    "org.flywaydb" % "flyway-core" % flyway
  )

  lazy val zookeeperDeps = Seq(
    "org.apache.curator" % "apache-curator" % curator % Provided
  )

  lazy val cassandraDeps = Seq(
    "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnector,

    // The following dependency is not required by jobserver. It is required by
    // C* connector (only if used with Hadoop 3.x). Once C* is compatible with
    // hadoop, the following dependency should be removed.
    // https://datastax-oss.atlassian.net/browse/SPARKC-566
    "commons-configuration" % "commons-configuration" % commonConfigurations
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % logback
  )

  lazy val scalaTestDep = "org.scalatest" %% "scalatest" % scalaTest % Test

  lazy val coreTestDeps = Seq(
    scalaTestDep,
    "com.typesafe.akka" %% "akka-testkit" % akka % Test,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % akka % Test,
    "io.spray" %% "spray-testkit" % spray % Test
  )

  lazy val miscTestDeps = Seq(
    "org.apache.hadoop" % "hadoop-hdfs" % hadoop % Test classifier "tests" excludeAll(excludeCurator),
    "org.apache.hadoop" % "hadoop-common" % hadoop % Test classifier "tests" excludeAll(excludeCurator),
    "org.apache.hadoop" % "hadoop-minicluster" % hadoop % Test excludeAll(excludeCurator),
    "org.apache.curator" % "curator-test" % curatorTest % Test excludeAll(excludeGuava)
  )

  lazy val integrationTestDeps = Seq(
    "com.typesafe" % "config" % typeSafeConfig,
    "org.scalatest" %% "scalatest" % scalaTest,
    "com.softwaremill.sttp" %% "core" % "1.6.3",
    "com.typesafe.play" %% "play-json" % "2.7.4"
  )

  lazy val securityDeps = Seq(
    "org.apache.shiro" % "shiro-core" % shiro
  )

  lazy val serverDeps = apiDeps
  lazy val apiDeps = sparkDeps ++ miscDeps :+ typeSafeConfigDeps :+ scalaTestDep

  val repos = Seq(
    "Typesafe Repo" at "https://repo.typesafe.com/typesafe/releases/",
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io"
  )
}
