import Dependencies._
import Release._

lazy val akkaApp = Project(id = "akka-app", base = file("akka-app"))
  .settings(description := "Common Akka application stack: metrics, tracing, logging, and more.")
  .settings(commonSettings)
  .settings(libraryDependencies ++= coreTestDeps ++ akkaDeps)
  .settings(publishSettings)

lazy val jobServer = Project(id = "job-server", base = file("job-server"))
  .settings(commonSettings)
  .settings(revolverSettings)
  .settings(Assembly.settings)
  .settings(
    description := "Spark as a Service: a RESTful job server for Apache Spark",
    libraryDependencies ++= sparkDeps ++ slickDeps ++ cassandraDeps ++ securityDeps ++ coreTestDeps ++ akkaHttp,
    test in Test <<= (test in Test)
      .dependsOn(packageBin in Compile in jobServerTestJar)
      .dependsOn(clean in Compile in jobServerTestJar)
      .dependsOn(buildPython in jobServerPython)
      .dependsOn(clean in Compile in jobServerPython),
    testOnly in Test <<= (testOnly in Test)
      .dependsOn(packageBin in Compile in jobServerTestJar)
      .dependsOn(clean in Compile in jobServerTestJar)
      .dependsOn(buildPython in jobServerPython)
      .dependsOn(clean in Compile in jobServerPython),
    console in Compile <<= Defaults.consoleTask(fullClasspath in Compile, console in Compile),
    fullClasspath in Compile <<= (fullClasspath in Compile).map { classpath =>
      extraJarPaths ++ classpath
    },
    test in assembly := {},
    fork in Test := true
  )
  .settings(publishSettings)
  .dependsOn(akkaApp, jobServerApi)

lazy val jobServerTestJar =
  Project(id = "job-server-tests", base = file("job-server-tests"))
    .settings(commonSettings)
    .settings(jobServerTestJarSettings)
    .settings(noPublishSettings)
    .dependsOn(jobServerApi)

lazy val jobServerApi =
  Project(id = "job-server-api", base = file("job-server-api"))
    .settings(commonSettings)
    .settings(publishSettings)

lazy val jobServerExtras =
  Project(id = "job-server-extras", base = file("job-server-extras"))
    .settings(commonSettings)
    .settings(Extras.settings)
    .settings(
      test in Test <<= (test in Test)
        .dependsOn(packageBin in Compile in jobServerTestJar)
        .dependsOn(clean in Compile in jobServerTestJar)
        .dependsOn(buildPython in jobServerPython)
        .dependsOn(buildPyExamples in jobServerPython)
        .dependsOn(clean in Compile in jobServerPython),
      testOnly in Test <<= (testOnly in Test)
        .dependsOn(packageBin in Compile in jobServerTestJar)
        .dependsOn(clean in Compile in jobServerTestJar)
        .dependsOn(buildPython in jobServerPython)
        .dependsOn(buildPyExamples in jobServerPython)
        .dependsOn(clean in Compile in jobServerPython)
    )
    .dependsOn(jobServerApi, jobServer % "compile->compile; test->test")

lazy val jobServerPython =
  Project(id = "job-server-python", base = file("job-server-python"))
    .settings(commonSettings)
    .settings(jobServerPythonSettings)
    .dependsOn(jobServerApi, akkaApp % "test")

lazy val root = Project(id = "root", base = file("."))
  .settings(commonSettings)
  .settings(ourReleaseSettings)
  .settings(noPublishSettings)
  .settings(rootSettings)
  .settings(Docker.settings(jobServerExtras))
  .aggregate(jobServer, jobServerApi, jobServerTestJar, akkaApp, jobServerExtras, jobServerPython)
  .dependsOn(jobServer, jobServerExtras)
  .enablePlugins(DockerPlugin)

lazy val testPython =
  taskKey[Unit]("Launch a sub process to run the Python tests")
lazy val buildPython =
  taskKey[Unit]("Build the python side of python support into an egg")
lazy val buildPyExamples =
  taskKey[Unit]("Build the examples of python jobs into an egg")

lazy val jobServerPythonSettings = revolverSettings ++ Assembly.settings ++ publishSettings ++ Seq(
  libraryDependencies ++= sparkPythonDeps,
  fork in Test := true,
  cancelable in Test := true,
  testPython := PythonTasks.testPythonTask(baseDirectory.value),
  buildPython := PythonTasks.buildPythonTask(baseDirectory.value, version.value),
  buildPyExamples := PythonTasks.buildExamplesTask(baseDirectory.value, version.value),
  assembly <<= assembly.dependsOn(buildPython)
)

lazy val jobServerTestJarSettings = Seq(
  libraryDependencies ++= sparkDeps ++ apiDeps,
  description := "Test jar for Spark Job Server",
  exportJars := true // use the jar instead of target/classes
)

lazy val rootSettings = Seq(
  // Must run Spark tests sequentially because they compete for port 4040!
  parallelExecution in Test := false,
  publishArtifact := false,
  concurrentRestrictions := Seq(
    Tags.limit(Tags.CPU, java.lang.Runtime.getRuntime.availableProcessors()),
    // limit to 1 concurrent test task, even across sub-projects
    // Note: some components of tests seem to have the "Untagged" tag rather than "Test" tag.
    // So, we limit the sum of "Test", "Untagged" tags to 1 concurrent
    Tags.limitSum(1, Tags.Test, Tags.Untagged)
  )
)

// To add an extra jar to the classpath when doing "re-start" for quick development, set the
// env var EXTRA_JAR to the absolute full path to the jar
lazy val extraJarPaths = Option(System.getenv("EXTRA_JAR"))
  .map(jarpath => Seq(Attributed.blank(file(jarpath))))
  .getOrElse(Nil)

// Create a default Scala style task to run with compiles
lazy val runScalaStyle = taskKey[Unit]("testScalaStyle")

lazy val commonSettings = Defaults.coreDefaultSettings ++ implicitlySettings ++ Seq(
  organization := "spark.jobserver",
  crossPaths := true,
  scalaVersion := sys.env.getOrElse("SCALA_VERSION", "2.11.11"),
  dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  runScalaStyle := {
    org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
  },
  (compile in Compile) <<= (compile in Compile) dependsOn runScalaStyle,
  scalacOptions := Seq(
    "-deprecation",
    "-feature",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-language:existentials"
  ),
  // For Building on Encrypted File Systems...
  scalacOptions ++= Seq("-Xmax-classfile-name", "128"),
  libraryDependencies ++= apiDeps,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  // We need to exclude jms/jmxtools/etc because it causes undecipherable SBT errors  :(
  ivyXML :=
    <dependencies>
      <exclude module="jms"/>
      <exclude module="jmxtools"/>
      <exclude module="jmxri"/>
    </dependencies>
) ++ scoverageSettings

lazy val scoverageSettings = {
  // Semicolon-separated list of regexs matching classes to exclude
  coverageExcludedPackages := ".+Benchmark.*"
}
