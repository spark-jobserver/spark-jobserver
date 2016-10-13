package spark.jobserver

import java.io.File
import java.nio.file.Paths

trait TestJarFinder {
  val versionRegex = """(\d\.\d+).*""".r
  val version = scala.util.Properties.versionNumberString match { case versionRegex(d) => d }
  val testJarBaseDir = "job-server-tests"
  val extrasJarBaseDir = "job-server-extras"
  lazy val testJarDir = testJarBaseDir + "/target/scala-" + version + "/"
  lazy val extrasJarDir = extrasJarBaseDir + "/target/scala-" + version + "/"
  val testEggBaseDir = "job-server-python"
  lazy val testEggDir = testEggBaseDir + "/target/python/"

  lazy val emptyBinaryBaseDir = "job-server"
  lazy val emptyBinaryDir = emptyBinaryBaseDir + "/target/scala-" + version + "/test-classes/test_binaries"

  //Guaranteed to be in place during test-phase, regardless of what other building/packaging has been done:
  lazy val emptyJar =
    new java.io.File(this.getClass.getClassLoader.getResource("test_binaries/empty.jar").getFile())
  lazy val emptyEgg =
    new java.io.File(this.getClass.getClassLoader.getResource("test_binaries/empty.egg").getFile())

  /**
    * Returns the base directory of a given package
    *
    * @param pkg
    * @return
    */
  def getBaseDir(pkg: String): String ={
    // Current directory.  Find out if we are in project root, and need to go up a level.
    val cwd = Paths.get(".").toAbsolutePath.normalize().toString
    val dotdot = if (Paths.get(cwd + s"/$pkg").toFile.isDirectory) "" else "../"
    s"$cwd/$dotdot"
  }

  /**
    * Returns a list of possible jars that match certain rules from a given directory
    * @param baseDir
    * @param jarDir
    * @return
    */
  def getJarsList(baseDir: String, jarDir: String): Seq[File] = {
    val candidates = new java.io.File(baseDir + jarDir).listFiles.toSeq
    candidates.filter { file =>
      val path = file.toString
      path.endsWith(".jar") && !path.endsWith("-tests.jar") && !path.endsWith("-sources.jar") &&
        !path.endsWith("-javadoc.jar") && !path.contains("scoverage")
    }
  }

  // Make testJar lazy so to give a chance for overriding of testJarDir to succeed
  lazy val testJar: java.io.File = {
    val allJars = getJarsList(getBaseDir(testJarBaseDir), testJarDir)
    assert(allJars.size == 1, allJars.toList.toString)
    allJars.head
  }

  lazy val testEgg: java.io.File = {
    val dir = new java.io.File(getBaseDir(testEggBaseDir) + testEggDir)
    val eggFiles = dir.listFiles().filter(_.getName.endsWith(".egg")).filter(_.getName.contains("examples"))
    assert(eggFiles.size == 1, eggFiles.toList.toString)
    eggFiles.head
  }

  lazy val extrasJar: java.io.File = {
    val allJars = getJarsList(getBaseDir(extrasJarBaseDir), extrasJarDir)
    assert(allJars.size == 1, allJars.toList.toString)
    allJars.head
  }
}