import sbtrelease.ReleasePlugin.autoImport._
import ReleaseTransformations._

object Release {

  lazy val settings = Seq(
    releaseProcess := Seq(
      checkSnapshotDependencies,
      runClean,
      runTest,
      inquireVersions,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )

}
