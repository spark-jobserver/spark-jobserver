import Dependencies._

ThisBuild / scalaVersion     := "2.11.11"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := ""
ThisBuild / organizationName := ""

lazy val root = (project in file("."))
  .settings(
    name := "job-server-integration-tests",
    libraryDependencies += scalaTest,
    libraryDependencies ++= helpers,

    mainClass in Compile := Some("spark.jobserver.integrationtests.IntegrationTests"),

  )
