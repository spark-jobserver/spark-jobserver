import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._

object Assembly {
  val blackList = List(
    "servlet-api",
    "guice-all",
    "junit",
    "uuid",
    "jetty",
    "jsp-api-2.0",
    "antlr",
    "avro",
    "slf4j-log4j",
    "log4j-1.2",
    "scala-actors",
    "spark",
    "commons-cli",
    "stax-api",
    "mockito"
  )
  lazy val settings = Seq(
    assemblyJarName in assembly := "spark-job-server.jar",
    // uncomment below to exclude tests
    // test in assembly := {},
    assemblyExcludedJars in assembly <<= (fullClasspath in assembly).map { dep =>
      dep.filter(cp => blackList.exists(cp.data.getName.startsWith(_)))
    },
    // We don't need the Scala library, Spark already includes it
    assembleArtifact in assemblyPackageScala := false,
    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf")     => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf"                               => MergeStrategy.concat
      case _                                              => MergeStrategy.first
    }
  )
}
