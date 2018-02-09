import scala.util.Properties.isJavaAtLeast

object Versions {
  lazy val spark = sys.env.getOrElse("SPARK_VERSION", "2.2.0")

  lazy val akka = "2.4.9"
  lazy val cassandra = "3.3.0"
  lazy val cassandraUnit = "2.2.2.1"
  lazy val commons = "1.4"
  lazy val derby = "10.12.1.1"
  lazy val flyway = "3.2.1"
  lazy val hadoop = "2.7.3"
  lazy val h2 = "1.3.176"
  lazy val java = sys.env.getOrElse("JAVA_VERSION", "8-jdk")
  lazy val jodaConvert = "1.8.1"
  lazy val jodaTime = "2.9.3"
  lazy val logback = "1.0.7"
  lazy val mesos = sys.env.getOrElse("MESOS_VERSION", "1.0.0-2.0.89.ubuntu1404")
  lazy val metrics = "2.2.0"
  lazy val netty = "4.0.44.Final"
  lazy val postgres = "9.4.1209"
  lazy val mysql = "5.1.42"
  lazy val py4j = "0.10.4"
  lazy val scalaTest = "3.0.1"
  lazy val scalatic = "3.0.1"
  lazy val shiro = "1.2.4"
  lazy val slick = "3.1.1"
  lazy val spray = "1.3.3"
  lazy val sprayJson = "1.3.2"
  lazy val typeSafeConfig = if (isJavaAtLeast("1.8")) "1.3.0" else "1.2.1"
  lazy val cassandraConnector = "2.0.5"
}
