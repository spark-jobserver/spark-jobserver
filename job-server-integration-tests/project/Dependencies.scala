import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"

  lazy val helpers = Seq(
    "com.softwaremill.sttp" %% "core" % "1.6.3",
    "com.typesafe.play" %% "play-json" % "2.7.4"
  )
}
