import sbt._
import Versions._
import ExclusionRules._
object Dependencies {

  lazy val typeSafeConfigDeps = "com.typesafe" % "config" % typeSafeConfig

  lazy val yammerDeps = "com.yammer.metrics" % "metrics-core" % metrics

  lazy val miscDeps = Seq(
    "org.scalactic" %% "scalactic" % scalatic,
    "org.joda" % "joda-convert" % jodaConvert,
    "joda-time" % "joda-time" % jodaTime
  )

  lazy val akkaDeps = Seq(
    // Akka is provided because Spark already includes it, and Spark's version is shaded so it's not safe
    // to use this one
    "com.typesafe.akka" %% "akka-slf4j" % akka % "provided",
    "com.typesafe.akka" %% "akka-cluster" % akka exclude("com.typesafe.akka", "akka-remote"),
    "io.spray" %% "spray-json" % sprayJson,
    "io.spray" %% "spray-can" % spray,
    "io.spray" %% "spray-caching" % spray,
    "io.spray" %% "spray-routing" % spray,
    "io.spray" %% "spray-client" % spray,
    yammerDeps
  )

  val javaVersion = sys.env.getOrElse("JAVA_VERSION", "7-jdk")

  val mesosVersion = sys.env.getOrElse("MESOS_VERSION", mesos)

  val sparkVersion = sys.env.getOrElse("SPARK_VERSION", spark)
  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    // Force netty version.  This avoids some Spark netty dependency problem.
    "io.netty" % "netty-all" % "4.0.37.Final"
  )

  lazy val sparkExtraDeps = Seq(
    "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided excludeAll(
      excludeNettyIo, excludeQQ, excludeScalaTest
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
    "commons-dbcp" % "commons-dbcp" % commons,
    "org.flywaydb" % "flyway-core" % flyway
  )

  lazy val cassandraDeps = Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % cassandra,
    "com.datastax.cassandra" % "cassandra-driver-mapping" % cassandra
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % logback
  )

  lazy val scalaTestDep = "org.scalatest" %% "scalatest" % scalaTest % Test

  lazy val coreTestDeps = Seq(
    scalaTestDep,
    "com.typesafe.akka" %% "akka-testkit" % akka % Test,
    "io.spray" %% "spray-testkit" % spray % Test,
    "org.cassandraunit" % "cassandra-unit" % cassandraUnit % Test
  )

  lazy val securityDeps = Seq(
     "org.apache.shiro" % "shiro-core" % shiro
  )

  lazy val serverDeps = apiDeps
  lazy val apiDeps = sparkDeps ++ miscDeps :+ typeSafeConfigDeps :+ scalaTestDep

  val repos = Seq(
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io"
  )
}
