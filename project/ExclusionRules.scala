import sbt._

object ExclusionRules {
  val excludeCglib = ExclusionRule(organization = "org.sonatype.sisu.inject")
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeScalaTest = ExclusionRule(organization = "org.scalatest")
  val excludeScala = ExclusionRule(organization = "org.scala-lang")
  val excludeAsm = ExclusionRule(organization = "asm")
  val excludeQQ = ExclusionRule(organization = "org.scalamacros")
  val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
  val excludeZookeeper = ExclusionRule(organization = "org.apache.zookeeper", name = "zookeeper")
  val excludeCurator = ExclusionRule(organization = "org.apache.curator")
}
