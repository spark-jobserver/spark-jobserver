import sbt.Keys._
import sbt._
import spray.revolver.RevolverPlugin.autoImport._

object Revolver {
  lazy val settings = Seq(
    javaOptions in reStart += Extras.jobServerLogging,
    // Give job server a bit more PermGen since it does classloading
    javaOptions in reStart += "-XX:MaxPermSize=256m",
    javaOptions in reStart += "-Djava.security.krb5.realm= -Djava.security.krb5.kdc=",
    // This lets us add Spark back to the classpath without assembly barfing
    fullClasspath in reStart := (fullClasspath in Compile).value,
    mainClass in reStart := Some("spark.jobserver.JobServer")
  )
}
