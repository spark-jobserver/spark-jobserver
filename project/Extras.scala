import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

object Extras {

  lazy val jobServerLogbackLogging = "-Dlogback.configurationFile=config/logback-local.xml"
  lazy val jobServerLogging        = "-Dlog4j.configuration=file:config/log4j-local.properties"

  lazy val settings = Revolver.settings ++ Assembly.settings ++ Release.publishSettings ++ Seq(
    libraryDependencies ++= Dependencies.sparkExtraDeps,
    test in Test <<= (test in Test)
      .dependsOn(packageBin in Compile)
      .dependsOn(clean in Compile),
    fork in Test := true,
    test in assembly := {},
    exportJars := true
  )
}
