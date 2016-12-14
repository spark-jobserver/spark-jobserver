import java.io.File

import scala.sys.process.Process

object PythonTasks {

  def workingDirectory(baseDirectory: File): File =
    new File(baseDirectory.getAbsolutePath + Seq("src", "python").mkString("/", "/", ""))

  def testPythonTask(baseDirectory: File): Unit = {
    val cwd = workingDirectory(baseDirectory)
    val exitCode = Process(cwd.getAbsolutePath + "/run-tests.sh", cwd).!
    if(exitCode != 0) {
      sys.error(s"Running python tests received non-zero exit code $exitCode")
    }
  }

  def buildPythonTask(baseDirectory: File, version: String): Unit = {
    val cwd = workingDirectory(baseDirectory)
    val exitCode = Process(Seq(cwd.getAbsolutePath + "/build.sh", version, "setup.py"), cwd).!
    if(exitCode != 0) {
      sys.error(s"Building python API received non-zero exit code $exitCode")
    }
  }

  def buildExamplesTask(baseDirectory: File, version: String): Unit = {
    val cwd = workingDirectory(baseDirectory)
    val exitCode = Process(Seq(cwd.getAbsolutePath + "/build.sh", version,  "setup-examples.py"), cwd).!
    if(exitCode != 0) {
      sys.error(s"Building python examples received non-zero exit code $exitCode")
    }
  }
}
