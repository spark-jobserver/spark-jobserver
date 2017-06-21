import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport.{assembly, assemblyOutputPath}
import sbtdocker.DockerPlugin.autoImport._

object Docker {
  def settings(extras: Project) = Seq(
    // Make the docker task depend on the assembly task, which generates a fat JAR file
    docker <<= (docker dependsOn (assembly in extras)),
    dockerfile in docker := {
      val artifact           = (assemblyOutputPath in assembly in extras).value
      val artifactTargetPath = s"/app/${artifact.name}"

      val sparkBuild = s"spark-${Versions.spark}"
      val sparkBuildCmd = scalaBinaryVersion.value match {
        case "2.11" =>
          "./make-distribution.sh -Dscala-2.11 -Phadoop-2.7 -Phive"
        case other => throw new RuntimeException(s"Scala version $other is not supported!")
      }

      new sbtdocker.mutable.Dockerfile {
        from(s"openjdk:${Versions.java}")
        // Dockerfile best practices: https://docs.docker.com/articles/dockerfile_best-practices/
        expose(8090)
        expose(9999) // for JMX
        env("MESOS_VERSION", Versions.mesos)
        runRaw(
          """echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" > /etc/apt/sources.list.d/mesosphere.list && \
                apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
                apt-get -y update && \
                apt-get -y install mesos=${MESOS_VERSION} && \
                apt-get clean
             """)
        env("MAVEN_VERSION", "3.3.9")
        runRaw(
          """mkdir -p /usr/share/maven /usr/share/maven/ref \
          && curl -fsSL http://apache.osuosl.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz \
          | tar -xzC /usr/share/maven --strip-components=1 \
          && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn
        """)
        env("MAVEN_HOME", "/usr/share/maven")
        env("MAVEN_CONFIG", "/.m2")

        copy(artifact, artifactTargetPath)
        copy(baseDirectory(_ / "bin" / "server_start.sh").value, file("app/server_start.sh"))
        copy(baseDirectory(_ / "bin" / "server_stop.sh").value, file("app/server_stop.sh"))
        copy(baseDirectory(_ / "bin" / "manager_start.sh").value, file("app/manager_start.sh"))
        copy(baseDirectory(_ / "bin" / "setenv.sh").value, file("app/setenv.sh"))
        copy(baseDirectory(_ / "config" / "log4j-stdout.properties").value, file("app/log4j-server.properties"))
        copy(baseDirectory(_ / "config" / "docker.conf").value, file("app/docker.conf"))
        copy(baseDirectory(_ / "config" / "docker.sh").value, file("app/settings.sh"))
        // Including envs in Dockerfile makes it easy to override from docker command
        env("JOBSERVER_MEMORY", "1G")
        env("SPARK_HOME", "/spark")
        // Use a volume to persist database between container invocations
        run("mkdir", "-p", "/database")
        runRaw(
          s"""
             |wget http://d3kbcqa49mib13.cloudfront.net/$sparkBuild.tgz && \\
             |tar -xvf $sparkBuild.tgz && \\
             |cd $sparkBuild && \\
             |$sparkBuildCmd && \\
             |cd .. && \\
             |mv $sparkBuild/dist /spark && \\
             |rm $sparkBuild.tgz && \\
             |rm -r $sparkBuild
        """.stripMargin.trim
        )
        volume("/database")
        entryPoint("app/server_start.sh")
      }
    },
    imageNames in docker := Seq(
      sbtdocker.ImageName(
        namespace = Some("velvia"),
        repository = "spark-jobserver",
        tag = Some(
          s"${version.value}" +
            s".mesos-${Versions.mesos.split('-')(0)}" +
            s".spark-${Versions.spark}" +
            s".scala-${scalaBinaryVersion.value}" +
            s".jdk-${Versions.java}")
      )
    )
  )
}
