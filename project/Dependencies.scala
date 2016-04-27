import sbt._

object Dependencies {
  val excludeCglib = ExclusionRule(organization = "org.sonatype.sisu.inject")
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeScalaTest = ExclusionRule(organization = "org.scalatest")
  val excludeScala= ExclusionRule(organization = "org.scala-lang")
  val excludeNettyIo = ExclusionRule(organization = "io.netty", artifact= "netty-all")
  val excludeAsm = ExclusionRule(organization = "asm")
  val excludeQQ = ExclusionRule(organization = "org.scalamacros")
  val jdkVersion = scala.util.Properties.isJavaAtLeast("1.8")

  lazy val typeSafeConfigDeps = if(jdkVersion){
    "com.typesafe" % "config" % "1.3.0"
  } else {
    "com.typesafe" % "config" % "1.2.1"
  }
  lazy val yammerDeps = "com.yammer.metrics" % "metrics-core" % "2.2.0"

  lazy val jodaDeps = Seq(
    "org.joda" % "joda-convert" % "1.8.1",
    "joda-time" % "joda-time" % "2.9.3"
  )

  lazy val akkaDeps = Seq(
    // Akka is provided because Spark already includes it, and Spark's version is shaded so it's not safe
    // to use this one
    "com.typesafe.akka" %% "akka-slf4j" % "2.3.15" % "provided",
    "com.typesafe.akka" %% "akka-cluster" % "2.3.15" exclude("com.typesafe.akka", "akka-remote"),
    "io.spray" %% "spray-json" % "1.3.2",
    "io.spray" %% "spray-can" % "1.3.3",
    "io.spray" %% "spray-caching" % "1.3.3",
    "io.spray" %% "spray-routing" % "1.3.3",
    "io.spray" %% "spray-client" % "1.3.3",
    yammerDeps
  ) ++ jodaDeps

  val javaVersion = sys.env.getOrElse("JAVA_VERSION", "7-jre")

  val mesosVersion = sys.env.getOrElse("MESOS_VERSION", "0.28.1-2.0.20.ubuntu1404")

  val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "1.6.1")
  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    // Force netty version.  This avoids some Spark netty dependency problem.
    "io.netty" % "netty-all" % "4.0.29.Final"
  )

  lazy val sparkExtraDeps = Seq(
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ),
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" excludeAll(excludeNettyIo, excludeQQ, excludeScalaTest)
  )

  lazy val slickDeps = Seq(
    "com.typesafe.slick" %% "slick" % "2.1.0",
    "com.h2database" % "h2" % "1.3.176",
    "commons-dbcp" % "commons-dbcp" % "1.4",
    "org.flywaydb" % "flyway-core" % "3.2.1"
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.0.7"
  )

  lazy val coreTestDeps = Seq(
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "com.typesafe.akka" %% "akka-testkit" % "2.3.15" % "test",
    "io.spray" %% "spray-testkit" % "1.3.3" % "test"
  )

  lazy val securityDeps = Seq(
     "org.apache.shiro" % "shiro-core" % "1.2.4"
  )

  lazy val serverDeps = apiDeps ++ jodaDeps
  lazy val apiDeps = sparkDeps :+ typeSafeConfigDeps

  val repos = Seq(
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io"
  )
}
