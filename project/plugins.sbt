resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("io.kamon" % "aspectj-runner" % "0.1.3")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.1.10")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.8.0")

addSbtPlugin("com.lucidchart" % "sbt-scalafmt-coursier" % "1.3")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-RC3")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.5.0")

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

addSbtPlugin("me.lessis" % "ls-sbt" % "0.1.3")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.0")
