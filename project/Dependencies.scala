import sbt._
import Keys._

object Dependencies {
  val excludeCglib = ExclusionRule(organization = "org.sonatype.sisu.inject")
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")
  val excludeAsm = ExclusionRule(organization = "asm")

  lazy val typeSafeConfigDeps = "com.typesafe" % "config" % "1.2.1"
  lazy val yammerDeps = "com.yammer.metrics" % "metrics-core" % "2.2.0"

  lazy val yodaDeps = Seq(
    "org.joda" % "joda-convert" % "1.7",
    "joda-time" % "joda-time" % "2.6"
  )

  lazy val akkaDeps = Seq(
    // Akka is provided because Spark already includes it, and Spark's version is shaded so it's not safe
    // to use this one
    "com.typesafe.akka" %% "akka-slf4j" % "2.3.4" % "provided",
    "io.spray" %% "spray-json" % "1.2.5",
    "io.spray" % "spray-can" % "1.3.1",
    "io.spray" % "spray-routing" % "1.3.1",
    "io.spray" % "spray-client" % "1.3.1",
    yammerDeps
  ) ++ yodaDeps

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % "1.2.0" % "provided" exclude("io.netty", "netty-all"),
    // Force netty version.  This avoids some Spark netty dependency problem.
    "io.netty" % "netty-all" % "4.0.23.Final"
  )

  lazy val slickDeps = Seq(
    "com.typesafe.slick" %% "slick" % "2.0.2",
    "com.h2database" % "h2" % "1.4.184"
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.2"
  )

  lazy val coreTestDeps = Seq(
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "com.typesafe.akka" %% "akka-testkit" % "2.3.4" % "test",
    "io.spray" % "spray-testkit" % "1.3.1" % "test"
  )


  lazy val serverDeps = apiDeps ++ yodaDeps
  lazy val apiDeps = sparkDeps :+ typeSafeConfigDeps

  val repos = Seq(
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io",
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    "Sogou Internal  release Repo" at "http://cloud.sogou-inc.com/nexus/content/repositories/Release",
    "Sogou Internal  Snapshot Repo" at "http://cloud.sogou-inc.com/nexus/content/repositories/snapshots/"
  )
}
