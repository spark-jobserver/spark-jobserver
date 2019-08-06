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

    unmanagedSourceDirectories in Compile += baseDirectory.value / "src/test/scala",
    unmanagedResourceDirectories in Compile += baseDirectory.value / "src/test/resources",
    mainClass in Compile := Some("spark.jobserver.integrationtests.IntegrationTests"),

    // Skip tests during assembly
    test in assembly := {}
  )
