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
    "io.netty" % "netty-all" % "4.0.29.Final"
  )

  lazy val sparkExtraDeps = Seq(
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ, excludeScalaTest)
  )

  lazy val slickDeps = Seq(
    "com.typesafe.slick" %% "slick" % slick,
    "com.h2database" % "h2" % h2,
    "commons-dbcp" % "commons-dbcp" % commons,
    "org.flywaydb" % "flyway-core" % flyway
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % logback
  )

  lazy val scalaTestDep = "org.scalatest" %% "scalatest" % scalaTest % "test"

  lazy val coreTestDeps = Seq(
    scalaTestDep,
    "com.typesafe.akka" %% "akka-testkit" % akka % "test",
    "io.spray" %% "spray-testkit" % spray % "test",
    "com.github.zxl0714" % "redis-mock" % "0.1"
  )

  lazy val securityDeps = Seq(
     "org.apache.shiro" % "shiro-core" % shiro
  )

  lazy val cacheDeps = Seq(
    "net.debasishg" %% "redisclient" % "3.0",
    "com.bionicspirit" %% "shade" % "1.7.4"
  )

  lazy val serverDeps = apiDeps ++ miscDeps
  lazy val apiDeps = sparkDeps ++ miscDeps :+ typeSafeConfigDeps

  val repos = Seq(
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io"
  )
}
