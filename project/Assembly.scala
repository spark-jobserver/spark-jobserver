import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._

object Assembly {
  lazy val settings = Seq(
    assembly / assemblyJarName := "spark-job-server.jar",
      // uncomment below to exclude tests
    // test in assembly := {},
    assembly / assemblyExcludedJars := (assembly / fullClasspath).map(_.filter { cp =>
      List("servlet-api", "guice-all", "junit", "uuid",
        "jetty", "jsp-api-2.0", "antlr", "avro", "slf4j-log4j", "log4j-1.2",
        "scala-actors", "commons-cli", "stax-api", "mockito").exists(cp.data.getName.startsWith(_))
    }).value,
    // We don't need the Scala library, Spark already includes it
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyMergeStrategy := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }//,
    // NOTE(velvia): Some users have reported NoClassDefFound errors due to this shading.
    // java.lang.NoClassDefFoundError: sjs/shapeless/Poly2$Case2Builder$$anon$3
    // This is disabled for now, if you really need it, re-enable it and good luck!
    // assembly / assemblyShadeRules := Seq(
    //   ShadeRule.rename("shapeless.**" -> "sjs.shapeless.@1").inAll
    // )
  )
}
