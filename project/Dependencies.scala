import sbt._
import Versions._
import ExclusionRules._

object Dependencies {

  lazy val typeSafeConfigDeps = "com.typesafe" % "config" % typeSafeConfig

  lazy val yammerDeps = "com.yammer.metrics" % "metrics-core" % metrics

  lazy val miscDeps = Seq(
    "org.scalactic" %% "scalactic"   % scalatic,
    "org.joda"      % "joda-convert" % jodaConvert,
    "joda-time"     % "joda-time"    % jodaTime
  )

  lazy val log4j = Seq(
    "org.apache.logging.log4j" % "log4j-core"       % "2.8.2",
    "org.apache.logging.log4j" % "log4j-api"        % "2.8.2",
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.8.2"
  )

  lazy val akkaHttp = Seq(
    "com.typesafe.akka" %% "akka-slf4j"           % akka,
    "com.typesafe.akka" %% "akka-http"            % `akka-http`,
    "com.typesafe.akka" %% "akka-http-spray-json" % `akka-http`,
    "com.typesafe.akka" %% "akka-http-testkit"    % `akka-http` % Test
  ) ++ log4j

  lazy val akkaDeps = Seq(
    "com.typesafe.akka" %% "akka-slf4j"                % akka,
    "com.typesafe.akka" %% "akka-cluster"              % akka exclude ("com.typesafe.akka", "akka-remote"),
    "io.spray"          %% "spray-json"                % sprayJson,
    "io.spray"          %% "spray-can"                 % spray,
    "io.spray"          %% "spray-caching"             % spray,
    "io.spray"          %% "spray-routing-shapeless23" % "1.3.4",
    "io.spray"          %% "spray-client"              % spray,
    yammerDeps
  )

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % spark % "provided" //excludeAll (excludeNettyIo, excludeQQ),
    // Force netty version.  This avoids some Spark netty dependency problem.
    //"io.netty" % "netty-all" % netty
  )

  lazy val sparkExtraDeps = Seq(
    excludeNettyQQ("org.apache.spark" %% "spark-mllib"     % spark % Provided),
    excludeNettyQQ("org.apache.spark" %% "spark-sql"       % spark % Provided),
    excludeNettyQQ("org.apache.spark" %% "spark-streaming" % spark % Provided),
    "org.apache.spark" %% "spark-hive" % spark % Provided excludeAll (
      excludeNettyIo, excludeQQ, excludeScalaTest
    )
  )

  lazy val sparkPythonDeps = Seq(
    "net.sf.py4j" % "py4j"        % py4j,
    "io.spray"    %% "spray-json" % sprayJson % Test
  ) ++ sparkExtraDeps

  lazy val slickDeps = Seq(
    "com.typesafe.slick" %% "slick"       % slick,
    "com.h2database"     % "h2"           % h2,
    "org.postgresql"     % "postgresql"   % postgres,
    "commons-dbcp"       % "commons-dbcp" % commons,
    "org.flywaydb"       % "flyway-core"  % flyway
  )

  lazy val cassandraDeps = Seq(
    "com.datastax.cassandra" % "cassandra-driver-core"    % cassandra,
    "com.datastax.cassandra" % "cassandra-driver-mapping" % cassandra
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % logback
  )

  lazy val coreTestDeps = Seq(
    "org.scalatest"     %% "scalatest"     % scalaTest     % Test,
    "com.typesafe.akka" %% "akka-testkit"  % akka          % Test,
    "io.spray"          %% "spray-testkit" % spray         % Test,
    "org.cassandraunit" % "cassandra-unit" % cassandraUnit % Test
  )

  lazy val securityDeps = Seq(
    "org.apache.shiro" % "shiro-core" % shiro
  )

  lazy val apiDeps = sparkDeps ++ miscDeps ++ coreTestDeps :+ typeSafeConfigDeps
}
